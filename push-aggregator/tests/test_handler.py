import json

import push_aggregator.handler as handler_module
from push_aggregator.handler import handle_event


class _FakeCursor:
    """SQL 実行内容を記録し、SELECT 応答を差し替えるスタブ。"""

    def __init__(self, connection):
        self._connection = connection
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self._connection.queries.append((normalized, params))

        if normalized.startswith("INSERT IGNORE INTO t_delivery_chunk_result"):
            self.rowcount = 0 if params[1] in self._connection.duplicated_chunk_ids else 1
        elif normalized.startswith("SELECT notification_id"):
            self._connection.selected = self._connection.execution_rows.pop(0)
            self.rowcount = 1
        else:
            self.rowcount = 1

    def fetchone(self):
        return self._connection.selected


class _FakeConnection:
    def __init__(self, execution_rows, duplicated_chunk_ids=()):
        self.queries = []
        self.execution_rows = list(execution_rows)
        self.duplicated_chunk_ids = set(duplicated_chunk_ids)
        self.selected = None
        self.committed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.committed = True


def _execution_row(**overrides):
    row = {
        "notification_id": 12345,
        "notification_type": 0,
        "triggered_by": 0,
        "content_version": 3,
        "total_count": 100,
        "success_count": 98,
        "fail_count": 1,
        "skipped_count": 1,
        "dispatch_completed": 1,
        "expected_chunk_count": 1,
        "processed_chunk_count": 1,
    }
    row.update(overrides)
    return row


def _record(chunk_id="1", invalid_endpoint_ids=None, message_id="m-1"):
    return {
        "messageId": message_id,
        "body": json.dumps(
            {
                "execution_id": "0190a0c0-0000-7000-8000-000000000000",
                "chunk_id": chunk_id,
                "success_count": 98,
                "fail_count": 1,
                "skipped_count": 1,
                "invalid_endpoint_ids": invalid_endpoint_ids or [],
                "error_samples": ["PERMANENT_FAILURE:DeviceUnregistered"],
            }
        ),
    }


def _queries_starting_with(connection, prefix):
    return [query for query in connection.queries if query[0].startswith(prefix)]


def test_handle_event_accepts_empty_records():
    """Records が空の場合は DB へ接続せず終了する。"""
    result = handle_event({"Records": []}, None)

    assert result == {"applied_chunk_count": 0, "completed_execution_ids": []}


def test_handle_event_completes_execution(monkeypatch):
    """完了条件を満たす実行を success へ遷移させ、親通知を completed にする。"""
    connection = _FakeConnection([_execution_row()])
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    result = handle_event({"Records": [_record(invalid_endpoint_ids=["endpoint-002"])]}, None)

    assert result["applied_chunk_count"] == 1
    assert result["completed_execution_ids"] == ["0190a0c0-0000-7000-8000-000000000000"]
    assert connection.committed is True

    finalize = _queries_starting_with(connection, "UPDATE t_delivery_execution SET result_status")
    assert finalize[0][1][0] == handler_module.RESULT_STATUS_SUCCESS

    notification = _queries_starting_with(connection, "UPDATE t_notification SET send_status")
    assert notification[0][1][0] == handler_module.SEND_STATUS_COMPLETED

    invalidate = _queries_starting_with(connection, "UPDATE t_user_device")
    assert invalidate[0][1] == ("endpoint-002",)


def test_handle_event_ignores_duplicated_chunk(monkeypatch):
    """再配送された既登録チャンクは加算対象から除外する。"""
    connection = _FakeConnection([], duplicated_chunk_ids={"1"})
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    result = handle_event({"Records": [_record()]}, None)

    assert result["applied_chunk_count"] == 0
    assert _queries_starting_with(connection, "UPDATE t_delivery_execution SET success_count") == []


def test_handle_event_keeps_recurring_notification_waiting(monkeypatch):
    """繰り返し送信では親通知の send_status を更新しない。"""
    connection = _FakeConnection([_execution_row(notification_type=2)])
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    handle_event({"Records": [_record()]}, None)

    assert _queries_starting_with(connection, "UPDATE t_notification SET send_status") == []


def test_handle_event_marks_partial_test_send_as_error(monkeypatch):
    """テスト送信は全件成功でない場合に error とし、テスト成功バージョンを更新しない。"""
    connection = _FakeConnection([_execution_row(triggered_by=2)])
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    handle_event({"Records": [_record()]}, None)

    finalize = _queries_starting_with(connection, "UPDATE t_delivery_execution SET result_status")
    assert finalize[0][1][0] == handler_module.RESULT_STATUS_ERROR
    assert (
        _queries_starting_with(connection, "UPDATE t_notification SET last_test_success_version")
        == []
    )


def test_handle_event_records_test_success_version(monkeypatch):
    """テスト送信が全件成功した場合はテスト成功バージョンを更新する。"""
    connection = _FakeConnection(
        [
            _execution_row(
                triggered_by=2,
                total_count=2,
                success_count=2,
                fail_count=0,
                skipped_count=0,
            )
        ]
    )
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    handle_event({"Records": [_record()]}, None)

    updates = _queries_starting_with(
        connection, "UPDATE t_notification SET last_test_success_version"
    )
    assert updates[0][1] == (3, handler_module.SYSTEM_USER, 12345, 3)


def test_handle_event_skips_finalize_when_chunks_remain(monkeypatch):
    """未処理チャンクが残る実行は終了状態へ遷移させない。"""
    connection = _FakeConnection([_execution_row(expected_chunk_count=5, processed_chunk_count=1)])
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)

    result = handle_event({"Records": [_record()]}, None)

    assert result["completed_execution_ids"] == []
    assert _queries_starting_with(connection, "UPDATE t_delivery_execution SET result_status") == []
