import json
import sys
from types import SimpleNamespace

import media_event_processor.handler as handler_module
from media_event_processor.handler import handle_event


def test_handle_event_accepts_empty_records():
    """Records が空の場合は処理件数 0 を返す。"""
    result = handle_event({"Records": []}, None)

    assert result == {"processed_count": 0}


def test_handle_event_publishes_image(monkeypatch):
    """画像 upload 処理では S3 copy 後に commit する。"""
    calls = []

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            calls.append((query, params))

        def fetchone(self):
            return {"resource_type": 1, "media_status": 1, "media_convert_job_id": None}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("commit", ()))

    class S3Client:
        def copy_object(self, **kwargs):
            calls.append(("copy_object", kwargs))

    monkeypatch.setenv("VIDEO_BUCKET_NAME", "media-bucket")
    monkeypatch.setattr(handler_module, "_connect_db", lambda: Connection())
    monkeypatch.setattr(handler_module, "_s3", lambda: S3Client())

    event = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "operation": "process-media-upload",
                        "mediaId": "01j7x8k2m6q4w9b3n5r1a0c8df",
                        "resourceType": 1,
                        "uploadKey": "uploads/image/01j7x8k2m6q4w9b3n5r1a0c8df/source.jpg",
                        "publicKey": "public/image/01j7x8k2m6q4w9b3n5r1a0c8df/source.jpg",
                    }
                )
            }
        ]
    }

    result = handle_event(event, None)

    assert result == {"processed_count": 1}
    assert ("commit", ()) in calls
    assert any(call[0] == "copy_object" for call in calls)


def test_handle_event_starts_video_conversion_with_zero_resource_type(monkeypatch):
    """動画 resourceType=0 を有効値として MediaConvert job を開始する。"""
    calls = []

    class Cursor:
        rowcount = 1

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            calls.append((query, params))

        def fetchone(self):
            return {"resource_type": 0, "media_status": 1, "media_convert_job_id": None}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("commit", ()))

    monkeypatch.setattr(handler_module, "_connect_db", lambda: Connection())
    monkeypatch.setattr(handler_module, "_create_mediaconvert_job", lambda *args: "job-123")

    event = {
        "Records": [
            {
                "body": json.dumps(
                    {
                        "operation": "process-media-upload",
                        "mediaId": "01j7x8k2m6q4w9b3n5r1a0c8dg",
                        "resourceType": 0,
                        "uploadKey": "uploads/video/01j7x8k2m6q4w9b3n5r1a0c8dg/source.mov",
                        "publicKey": "public/video/01j7x8k2m6q4w9b3n5r1a0c8dg/hls/master.m3u8",
                    }
                )
            }
        ]
    }

    result = handle_event(event, None)

    assert result == {"processed_count": 1}
    assert ("commit", ()) in calls
    assert any(params[0] == "PENDING" for _, params in calls if params)
    assert any(params[0] == "job-123" for _, params in calls if params)


def test_handle_event_marks_media_failed_on_final_attempt(monkeypatch):
    """SQS 最終受信時の処理失敗では media_status を failed に更新する。"""
    calls = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query, params):
            calls.append((query, params))

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def cursor(self):
            return Cursor()

        def commit(self):
            calls.append(("commit", ()))

    def fail_publish_image(*args):
        raise RuntimeError("copy failed")

    monkeypatch.setenv("EVENT_DLQ_MAX_RECEIVE_COUNT", "3")
    monkeypatch.setattr(handler_module, "_publish_image", fail_publish_image)
    monkeypatch.setattr(handler_module, "_connect_db", lambda: Connection())

    event = {
        "Records": [
            {
                "attributes": {"ApproximateReceiveCount": "3"},
                "body": json.dumps(
                    {
                        "operation": "process-media-upload",
                        "mediaId": "01j7x8k2m6q4w9b3n5r1a0c8df",
                        "resourceType": 1,
                        "uploadKey": "uploads/image/01j7x8k2m6q4w9b3n5r1a0c8df/source.jpg",
                        "publicKey": "public/image/01j7x8k2m6q4w9b3n5r1a0c8df/source.jpg",
                    }
                ),
            }
        ]
    }

    result = handle_event(event, None)

    assert result == {"processed_count": 1}
    assert ("commit", ()) in calls
    assert any(params == (3, "system:event-processor", "01j7x8k2m6q4w9b3n5r1a0c8df") for _, params in calls)


def test_create_mediaconvert_job_uses_public_key_basename_for_hls_destination(monkeypatch):
    """HLS 出力 manifest 名は publicKey の master.m3u8 に合わせる。"""
    calls = []

    class MediaConvertClient:
        def create_job(self, **kwargs):
            calls.append(kwargs)
            return {"Job": {"Id": "job-123"}}

    monkeypatch.setenv("VIDEO_BUCKET_NAME", "media-bucket")
    monkeypatch.setenv("MEDIACONVERT_ROLE_ARN", "arn:aws:iam::123456789012:role/mediaconvert-job-role")
    monkeypatch.setattr(handler_module, "_mediaconvert", lambda: MediaConvertClient())

    result = handler_module._create_mediaconvert_job(
        "01j7x8k2m6q4w9b3n5r1a0c8dg",
        "uploads/video/01j7x8k2m6q4w9b3n5r1a0c8dg/source.mov",
        "public/video/01j7x8k2m6q4w9b3n5r1a0c8dg/hls/master.m3u8",
    )

    assert result == "job-123"
    hls_settings = calls[0]["Settings"]["OutputGroups"][0]["OutputGroupSettings"]["HlsGroupSettings"]
    assert hls_settings["Destination"] == (
        "s3://media-bucket/public/video/01j7x8k2m6q4w9b3n5r1a0c8dg/hls/master"
    )
    assert calls[0]["Settings"]["OutputGroups"][0]["Outputs"][0]["NameModifier"] == "_media"


def test_mediaconvert_client_discovers_endpoint(monkeypatch):
    """MediaConvert client 作成前に account 固有 endpoint を解決する。"""
    calls = []

    class DiscoveryClient:
        def describe_endpoints(self, **kwargs):
            calls.append(("describe_endpoints", kwargs))
            return {"Endpoints": [{"Url": "https://account.mediaconvert.ap-northeast-1.amazonaws.com"}]}

    class MediaConvertClient:
        pass

    def client(service_name, **kwargs):
        calls.append(("client", service_name, kwargs))
        if "endpoint_url" in kwargs:
            return MediaConvertClient()
        return DiscoveryClient()

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=client))
    monkeypatch.setattr(handler_module, "_mediaconvert_client", None)
    monkeypatch.setattr(handler_module, "_mediaconvert_endpoint_url", None)

    result = handler_module._mediaconvert()

    assert isinstance(result, MediaConvertClient)
    assert calls == [
        ("client", "mediaconvert", {}),
        ("describe_endpoints", {"MaxResults": 1}),
        (
            "client",
            "mediaconvert",
            {"endpoint_url": "https://account.mediaconvert.ap-northeast-1.amazonaws.com"},
        ),
    ]
