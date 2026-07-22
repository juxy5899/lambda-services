import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

SYSTEM_USER = "system:event-processor"
MEDIA_STATUS_PROCESSING = 1
MEDIA_STATUS_PUBLISHED = 2
MEDIA_STATUS_FAILED = 3
RESOURCE_TYPE_VIDEO = 0
RESOURCE_TYPE_IMAGE = 1
PENDING_JOB_ID = "PENDING"

_s3_client = None
_mediaconvert_client = None
_mediaconvert_endpoint_url = None
_secrets_client = None
_db_secret: dict[str, Any] | None = None


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int]:
    records = event.get("Records") or []
    processed_count = 0

    for record in records:
        payload = _parse_sqs_record(record)
        detail = payload.get("detail") or {}
        operation = payload.get("operation") or detail.get("operation")
        source = payload.get("source")

        if operation == "process-media-upload":
            _process_media_upload(payload, detail)
        elif source == "aws.mediaconvert":
            _process_mediaconvert_callback(payload, detail)
        else:
            logger.info(
                "unsupported media event received",
                extra={"source": source or "unknown", "event_id": payload.get("id", "unknown")},
            )

        processed_count += 1

    return {"processed_count": processed_count}


def _parse_sqs_record(record: dict[str, Any]) -> dict[str, Any]:
    body = record.get("body")
    if not isinstance(body, str):
        raise ValueError("SQS record body must be a string")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"failed to parse SQS record body: {error}") from error


def _process_media_upload(payload: dict[str, Any], detail: dict[str, Any]) -> None:
    media_id = _required_string(payload.get("mediaId") or detail.get("mediaId"), "mediaId")
    resource_type = _required_int(payload.get("resourceType") or detail.get("resourceType"), "resourceType")
    upload_key = _required_string(payload.get("uploadKey") or detail.get("uploadKey"), "uploadKey")
    public_key = _required_string(payload.get("publicKey") or detail.get("publicKey"), "publicKey")

    logger.info(
        "media upload processing requested",
        extra={
            "media_id": media_id,
            "resource_type": resource_type,
        },
    )

    if resource_type == RESOURCE_TYPE_IMAGE:
        _publish_image(media_id, upload_key, public_key)
    elif resource_type == RESOURCE_TYPE_VIDEO:
        _start_video_conversion(media_id, upload_key, public_key)
    else:
        raise ValueError(f"unsupported resourceType: {resource_type}")


def _process_mediaconvert_callback(payload: dict[str, Any], detail: dict[str, Any]) -> None:
    job_id = _required_string(detail.get("jobId") or detail.get("job_id"), "jobId")
    status = _required_string(detail.get("status"), "status")

    logger.info(
        "mediaconvert callback received",
        extra={
            "detail_type": payload.get("detail-type"),
            "status": status,
            "job_id": job_id,
        },
    )

    if status == "COMPLETE":
        _update_video_status_by_job_id(job_id, MEDIA_STATUS_PUBLISHED)
    elif status in {"ERROR", "CANCELED"}:
        _update_video_status_by_job_id(job_id, MEDIA_STATUS_FAILED)


def _publish_image(media_id: str, upload_key: str, public_key: str) -> None:
    bucket_name = _required_env("VIDEO_BUCKET_NAME")

    with _connect_db() as connection:
        media = _fetch_media_for_update(connection, media_id)
        if not _is_processable_media(media, RESOURCE_TYPE_IMAGE):
            return

        _s3().copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": upload_key},
            Key=public_key,
        )
        _update_media_status(connection, media_id, MEDIA_STATUS_PUBLISHED)
        connection.commit()


def _start_video_conversion(media_id: str, upload_key: str, public_key: str) -> None:
    with _connect_db() as connection:
        media = _fetch_media_for_update(connection, media_id)
        if not _is_processable_media(media, RESOURCE_TYPE_VIDEO):
            return

        current_job_id = media.get("media_convert_job_id")
        if current_job_id and current_job_id != PENDING_JOB_ID:
            return
        if current_job_id == PENDING_JOB_ID:
            raise RuntimeError(f"MediaConvert job creation is already pending: {media_id}")

        acquired = _acquire_mediaconvert_job_creation(connection, media_id)
        if not acquired:
            raise RuntimeError(f"failed to acquire MediaConvert job creation: {media_id}")

        job_id = _create_mediaconvert_job(media_id, upload_key, public_key)
        _update_mediaconvert_job_id(connection, media_id, job_id)
        connection.commit()


def _fetch_media_for_update(connection: Any, media_id: str) -> dict[str, Any] | None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT media_id, resource_type, media_status, media_convert_job_id
            FROM t_media
            WHERE media_id = %s
              AND is_deleted = 0
            FOR UPDATE
            """,
            (media_id,),
        )
        return cursor.fetchone()


def _is_processable_media(media: dict[str, Any] | None, resource_type: int) -> bool:
    if media is None:
        return False

    return (
        media.get("resource_type") == resource_type
        and media.get("media_status") == MEDIA_STATUS_PROCESSING
    )


def _update_media_status(connection: Any, media_id: str, media_status: int) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE t_media
            SET media_status = %s,
                updated_by = %s
            WHERE media_id = %s
              AND media_status = 1
              AND is_deleted = 0
            """,
            (media_status, SYSTEM_USER, media_id),
        )


def _acquire_mediaconvert_job_creation(connection: Any, media_id: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE t_media
            SET media_convert_job_id = %s,
                updated_by = %s
            WHERE media_id = %s
              AND resource_type = 0
              AND media_status = 1
              AND media_convert_job_id IS NULL
              AND is_deleted = 0
            """,
            (PENDING_JOB_ID, SYSTEM_USER, media_id),
        )
        return cursor.rowcount == 1


def _update_mediaconvert_job_id(connection: Any, media_id: str, job_id: str) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE t_media
            SET media_convert_job_id = %s,
                updated_by = %s
            WHERE media_id = %s
              AND media_status = 1
              AND media_convert_job_id = %s
              AND is_deleted = 0
            """,
            (job_id, SYSTEM_USER, media_id, PENDING_JOB_ID),
        )
        if cursor.rowcount != 1:
            raise RuntimeError(f"failed to store MediaConvert job id: {media_id}")


def _update_video_status_by_job_id(job_id: str, media_status: int) -> None:
    with _connect_db() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                UPDATE t_media
                SET media_status = %s,
                    updated_by = %s
                WHERE resource_type = 0
                  AND media_status = 1
                  AND media_convert_job_id = %s
                  AND is_deleted = 0
                """,
                (media_status, SYSTEM_USER, job_id),
            )
        connection.commit()


def _create_mediaconvert_job(media_id: str, upload_key: str, public_key: str) -> str:
    bucket_name = _required_env("VIDEO_BUCKET_NAME")
    role_arn = _required_env("MEDIACONVERT_ROLE_ARN")
    output_prefix = _public_video_hls_prefix(public_key)

    response = _mediaconvert().create_job(
        Role=role_arn,
        Settings={
            "Inputs": [
                {
                    "FileInput": f"s3://{bucket_name}/{upload_key}",
                    "AudioSelectors": {"Audio Selector 1": {"DefaultSelection": "DEFAULT"}},
                    "VideoSelector": {},
                }
            ],
            "OutputGroups": [
                {
                    "Name": "HLS",
                    "OutputGroupSettings": {
                        "Type": "HLS_GROUP_SETTINGS",
                        "HlsGroupSettings": {
                            "Destination": f"s3://{bucket_name}/{output_prefix}",
                            "SegmentLength": 10,
                            "MinSegmentLength": 0,
                        },
                    },
                    "Outputs": [
                        {
                            "NameModifier": "master",
                            "ContainerSettings": {"Container": "M3U8"},
                            "VideoDescription": {
                                "CodecSettings": {
                                    "Codec": "H_264",
                                    "H264Settings": {
                                        "RateControlMode": "QVBR",
                                        "QvbrSettings": {"QvbrQualityLevel": 7},
                                        "MaxBitrate": 5000000,
                                    },
                                }
                            },
                            "AudioDescriptions": [
                                {
                                    "AudioSourceName": "Audio Selector 1",
                                    "CodecSettings": {
                                        "Codec": "AAC",
                                        "AacSettings": {
                                            "Bitrate": 96000,
                                            "CodingMode": "CODING_MODE_2_0",
                                            "SampleRate": 48000,
                                        },
                                    },
                                }
                            ],
                        }
                    ],
                }
            ],
        },
        UserMetadata={"mediaId": media_id},
    )
    return _required_string(response.get("Job", {}).get("Id"), "Job.Id")


def _public_video_hls_prefix(public_key: str) -> str:
    suffix = "/master.m3u8"
    if not public_key.endswith(suffix):
        raise ValueError(f"video publicKey must end with {suffix}: {public_key}")

    return public_key[: -len("master.m3u8")]


def _connect_db() -> Any:
    import pymysql

    secret = _load_db_secret()
    return pymysql.connect(
        host=_required_string(secret.get("host"), "secret.host"),
        user=_required_string(secret.get("username"), "secret.username"),
        password=_required_string(secret.get("password"), "secret.password"),
        database=_required_string(secret.get("dbname"), "secret.dbname"),
        port=int(secret.get("port") or 3306),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def _load_db_secret() -> dict[str, Any]:
    global _db_secret

    if _db_secret is None:
        response = _secretsmanager().get_secret_value(SecretId=_required_env("DB_SECRET_ARN"))
        secret_string = _required_string(response.get("SecretString"), "SecretString")
        _db_secret = json.loads(secret_string)

    return _db_secret


def _s3() -> Any:
    global _s3_client

    if _s3_client is None:
        import boto3

        _s3_client = boto3.client("s3")

    return _s3_client


def _mediaconvert() -> Any:
    global _mediaconvert_client
    global _mediaconvert_endpoint_url

    if _mediaconvert_client is None:
        import boto3

        if _mediaconvert_endpoint_url is None:
            _mediaconvert_endpoint_url = _discover_mediaconvert_endpoint(boto3.client("mediaconvert"))
        _mediaconvert_client = boto3.client("mediaconvert", endpoint_url=_mediaconvert_endpoint_url)

    return _mediaconvert_client


def _discover_mediaconvert_endpoint(client: Any) -> str:
    response = client.describe_endpoints(MaxResults=1)
    endpoints = response.get("Endpoints") or []
    if not endpoints:
        raise ValueError("MediaConvert endpoint was not returned")

    return _required_string(endpoints[0].get("Url"), "MediaConvert endpoint URL")


def _secretsmanager() -> Any:
    global _secrets_client

    if _secrets_client is None:
        import boto3

        _secrets_client = boto3.client("secretsmanager")

    return _secrets_client


def _required_env(name: str) -> str:
    return _required_string(os.environ.get(name), name)


def _required_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} is required")

    return value


def _required_int(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)

    raise ValueError(f"{name} must be an integer")
