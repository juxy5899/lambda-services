import json

import push_worker.handler as handler_module
from push_worker.handler import handle_event


class _FakePinpoint:
    """send_messages の呼び出し内容を記録するスタブ。"""

    def __init__(self, endpoint_results):
        self.endpoint_results = endpoint_results
        self.requests = []

    def send_messages(self, **kwargs):
        self.requests.append(kwargs)
        return {"MessageResponse": {"EndpointResult": self.endpoint_results}}


class _FakeSqs:
    """Aggregator へ送信したメッセージを記録するスタブ。"""

    def __init__(self):
        self.messages = []

    def send_message(self, **kwargs):
        self.messages.append(kwargs)


def _chunk_body(targets, send_type=0):
    return json.dumps(
        {
            "execution_id": "0190a0c0-0000-7000-8000-000000000000",
            "chunk_id": "1",
            "notification_id": 12345,
            "content_version": 3,
            "send_type": send_type,
            "notification": {
                "title": "通知タイトル",
                "body": "通知本文",
                "image_url": "https://example.invalid/image.jpg",
                "redirect_url": "app://notifications/12345",
            },
            "targets": targets,
        }
    )


def _setup(monkeypatch, endpoint_results):
    pinpoint = _FakePinpoint(endpoint_results)
    sqs = _FakeSqs()
    monkeypatch.setenv("PUSH_APPLICATION_ID", "app-0001")
    monkeypatch.setenv("AGGREGATOR_QUEUE_URL", "https://sqs.invalid/aggregator")
    monkeypatch.setenv("SEND_BATCH_SIZE", "2")
    monkeypatch.setattr(handler_module, "_pinpoint", lambda: pinpoint)
    monkeypatch.setattr(handler_module, "_sqs", lambda: sqs)
    return pinpoint, sqs


def test_handle_event_accepts_empty_records():
    """Records が空の場合は再処理対象なしを返す。"""
    assert handle_event({"Records": []}, None) == {"batchItemFailures": []}


def test_handle_event_aggregates_delivery_result(monkeypatch):
    """成功・失敗・スキップを分類し、Aggregator へ集計結果を送信する。"""
    pinpoint, sqs = _setup(
        monkeypatch,
        {
            "endpoint-001": {"DeliveryStatus": "SUCCESSFUL"},
            "endpoint-002": {
                "DeliveryStatus": "PERMANENT_FAILURE",
                "StatusMessage": "DeviceUnregistered",
            },
            "endpoint-003": {"DeliveryStatus": "OPT_OUT"},
        },
    )

    event = {
        "Records": [
            {
                "messageId": "m-1",
                "body": _chunk_body(
                    [
                        {"mypage_id": "M000001", "platform": 0, "endpoint_id": "endpoint-001"},
                        {"mypage_id": "M000002", "platform": 1, "endpoint_id": "endpoint-002"},
                        {"mypage_id": "M000003", "platform": 1, "endpoint_id": "endpoint-003"},
                        {"mypage_id": "M000004", "platform": 1},
                    ]
                ),
            }
        ]
    }

    result = handle_event(event, None)

    assert result == {"batchItemFailures": []}
    # SEND_BATCH_SIZE=2 のため 3 エンドポイントは 2 回に分割して送信される
    assert len(pinpoint.requests) == 2

    message = json.loads(sqs.messages[0]["MessageBody"])
    assert message["execution_id"] == "0190a0c0-0000-7000-8000-000000000000"
    assert message["chunk_id"] == "1"
    assert message["success_count"] == 1
    assert message["fail_count"] == 1
    # endpoint_id 未設定の対象と OPT_OUT がスキップとして計上される
    assert message["skipped_count"] == 2
    assert message["invalid_endpoint_ids"] == ["endpoint-002"]


def test_handle_event_includes_tracking_ids_in_push_payload(monkeypatch):
    """Push のカスタムデータに notification_id と execution_id を必ず含める。"""
    pinpoint, _ = _setup(monkeypatch, {"endpoint-001": {"DeliveryStatus": "SUCCESSFUL"}})

    event = {
        "Records": [
            {
                "messageId": "m-1",
                "body": _chunk_body(
                    [{"mypage_id": "M000001", "platform": 0, "endpoint_id": "endpoint-001"}]
                ),
            }
        ]
    }
    handle_event(event, None)

    configuration = pinpoint.requests[0]["MessageRequest"]["MessageConfiguration"]
    for key in ("APNSMessage", "GCMMessage", "DefaultPushNotificationMessage"):
        data = configuration[key]["Data"]
        assert data["notification_id"] == "12345"
        assert data["execution_id"] == "0190a0c0-0000-7000-8000-000000000000"


def test_handle_event_skips_notice_only_chunk(monkeypatch):
    """お知らせのみのチャンクは送信せずスキップとして報告する。"""
    pinpoint, sqs = _setup(monkeypatch, {})

    event = {
        "Records": [
            {
                "messageId": "m-1",
                "body": _chunk_body(
                    [{"mypage_id": "M000001", "platform": 0, "endpoint_id": "endpoint-001"}],
                    send_type=1,
                ),
            }
        ]
    }
    handle_event(event, None)

    assert pinpoint.requests == []
    message = json.loads(sqs.messages[0]["MessageBody"])
    assert message["skipped_count"] == 1
    assert message["success_count"] == 0


def test_handle_event_reports_batch_item_failure_on_send_error(monkeypatch):
    """送信が再試行上限まで失敗した場合は該当メッセージのみ再処理対象とする。"""
    _, sqs = _setup(monkeypatch, {})
    monkeypatch.setattr(handler_module, "RETRY_BASE_WAIT_SECONDS", 0)

    class FailingPinpoint:
        def send_messages(self, **kwargs):
            raise RuntimeError("throttled")

    monkeypatch.setattr(handler_module, "_pinpoint", lambda: FailingPinpoint())

    event = {
        "Records": [
            {
                "messageId": "m-1",
                "body": _chunk_body(
                    [{"mypage_id": "M000001", "platform": 0, "endpoint_id": "endpoint-001"}]
                ),
            }
        ]
    }
    result = handle_event(event, None)

    assert result == {"batchItemFailures": [{"itemIdentifier": "m-1"}]}
    assert sqs.messages == []
