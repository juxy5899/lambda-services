import timeout_monitor.handler as handler_module
from timeout_monitor.handler import handle_event

import pytest


class _FakeCursor:
    def __init__(self, connection):
        self._connection = connection
        self.rowcount = 1
        self._rows: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self._connection.queries.append((normalized, params))

        if normalized.startswith("SELECT id , notification_id"):
            self._rows = self._connection.stale_executions
            self.rowcount = len(self._rows)
        elif normalized.startswith("SELECT id , display_end_at"):
            self._rows = self._connection.recurring_notifications
            self.rowcount = len(self._rows)
        else:
            self.rowcount = self._connection.update_rowcount

    def fetchall(self):
        return self._rows


class _FakeConnection:
    def __init__(self, stale_executions=(), recurring_notifications=(), update_rowcount=1):
        self.queries = []
        self.stale_executions = list(stale_executions)
        self.recurring_notifications = list(recurring_notifications)
        self.update_rowcount = update_rowcount
        self.commits = 0
        self.rollbacks = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


class _FakeScheduler:
    def __init__(self, delete_error=None, schedule=None):
        self.deleted = []
        self.updated = []
        self._delete_error = delete_error
        self._schedule = schedule or {
            "ScheduleExpression": "cron(0 9 * * ? *)",
            "ScheduleExpressionTimezone": "Asia/Tokyo",
            "FlexibleTimeWindow": {"Mode": "OFF"},
            "Target": {"Arn": "arn:aws:sqs:ap-northeast-1:111111111111:q", "RoleArn": "arn:role"},
        }

    def delete_schedule(self, **kwargs):
        if self._delete_error is not None:
            raise self._delete_error
        self.deleted.append(kwargs)

    def get_schedule(self, **kwargs):
        return dict(self._schedule)

    def update_schedule(self, **kwargs):
        self.updated.append(kwargs)


class ResourceNotFoundException(Exception):
    """botocore が動的生成する例外クラス名を模したスタブ。"""


@pytest.fixture(autouse=True)
def _scheduler_env(monkeypatch):
    monkeypatch.setenv("SCHEDULER_GROUP_NAME", "MTI-AsahimyappSystem-dev-push-notification")


def _queries_starting_with(connection, prefix):
    return [query for query in connection.queries if query[0].startswith(prefix)]


def test_handle_event_runs_all_handlers(monkeypatch):
    """3 ハンドラを実行し、それぞれの結果件数を返す。"""
    connection = _FakeConnection()
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: _FakeScheduler())

    result = handle_event({}, None)

    assert result == {
        "csv_task_failed_count": 1,
        "execution_timeout_count": 0,
        "recurring_completed_count": 0,
    }
    assert connection.commits == 3
    assert _queries_starting_with(connection, "UPDATE t_csv_import_task")


def test_handle_event_marks_stale_execution_as_error(monkeypatch):
    """滞留した正式配信を error にし、親通知も error へ更新する。"""
    connection = _FakeConnection(
        stale_executions=[
            {
                "id": "0190a0c0-0000-7000-8000-000000000000",
                "notification_id": 12345,
                "notification_type": 1,
                "triggered_by": 1,
            }
        ]
    )
    scheduler = _FakeScheduler()
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: scheduler)

    result = handle_event({}, None)

    assert result["execution_timeout_count"] == 1
    notification_updates = _queries_starting_with(connection, "UPDATE t_notification SET send_status")
    assert notification_updates[0][1][0] == handler_module.SEND_STATUS_ERROR
    # 予約送信ではスケジュールの無効化は行わない
    assert scheduler.updated == []


def test_handle_event_disables_schedule_for_stale_recurring_execution(monkeypatch):
    """繰り返し送信が滞留した場合はスケジュールを DISABLED へ更新する。"""
    connection = _FakeConnection(
        stale_executions=[
            {
                "id": "0190a0c0-0000-7000-8000-000000000000",
                "notification_id": 12345,
                "notification_type": 2,
                "triggered_by": 1,
            }
        ]
    )
    scheduler = _FakeScheduler()
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: scheduler)

    handle_event({}, None)

    assert scheduler.updated[0]["Name"] == "notification_12345_recurring"
    assert scheduler.updated[0]["State"] == "DISABLED"


def test_handle_event_keeps_notification_for_stale_test_execution(monkeypatch):
    """テスト送信の滞留では実行のみ error とし、親通知は更新しない。"""
    connection = _FakeConnection(
        stale_executions=[
            {
                "id": "0190a0c0-0000-7000-8000-000000000000",
                "notification_id": 12345,
                "notification_type": 0,
                "triggered_by": 2,
            }
        ]
    )
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: _FakeScheduler())

    handle_event({}, None)

    assert _queries_starting_with(connection, "UPDATE t_notification SET send_status") == []


def test_handle_event_completes_recurring_after_display_end(monkeypatch):
    """表示終了後の繰り返し通知はスケジュール削除後に completed へ更新する。"""
    connection = _FakeConnection(
        recurring_notifications=[
            {"id": 12345, "display_end_at": "2026-07-01 00:00:00", "is_overdue": 0}
        ]
    )
    scheduler = _FakeScheduler()
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: scheduler)

    result = handle_event({}, None)

    assert result["recurring_completed_count"] == 1
    assert scheduler.deleted[0]["Name"] == "notification_12345_recurring"
    updates = _queries_starting_with(connection, "UPDATE t_notification SET send_status")
    assert updates[0][1][0] == handler_module.SEND_STATUS_COMPLETED


def test_handle_event_treats_missing_schedule_as_deleted(monkeypatch):
    """既に削除済みのスケジュールは成功として扱い、通知を completed にする。"""
    connection = _FakeConnection(
        recurring_notifications=[
            {"id": 12345, "display_end_at": "2026-07-01 00:00:00", "is_overdue": 0}
        ]
    )
    scheduler = _FakeScheduler(delete_error=ResourceNotFoundException("not found"))
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: scheduler)

    result = handle_event({}, None)

    assert result["recurring_completed_count"] == 1


def test_handle_event_raises_when_overdue_schedule_delete_keeps_failing(monkeypatch):
    """長時間解消しないスケジュール削除失敗は例外として監視通知の契機にする。"""
    connection = _FakeConnection(
        recurring_notifications=[
            {"id": 12345, "display_end_at": "2026-07-01 00:00:00", "is_overdue": 1}
        ]
    )
    scheduler = _FakeScheduler(delete_error=RuntimeError("throttled"))
    monkeypatch.setattr(handler_module, "_connect_db", lambda: connection)
    monkeypatch.setattr(handler_module, "_scheduler", lambda: scheduler)

    with pytest.raises(RuntimeError):
        handle_event({}, None)

    # CSV / 配信実行ハンドラは commit 済みで、失敗した繰り返しハンドラのみ rollback される
    assert connection.commits == 2
    assert connection.rollbacks == 1
