import json
import sys
from types import SimpleNamespace

import media_event_processor.handler as handler_module
from media_event_processor.handler import handle_event


def test_handle_event_accepts_empty_records():
    result = handle_event({"Records": []}, None)

    assert result == {"processed_count": 0}


def test_handle_event_publishes_image(monkeypatch):
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


def test_mediaconvert_client_discovers_endpoint(monkeypatch):
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
