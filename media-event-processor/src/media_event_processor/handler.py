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
    """SQS から受信した media 処理イベントを順次処理する。"""
    records = event.get("Records") or []
    processed_count = 0

    for record in records:
        payload = _parse_sqs_record(record)
        detail = payload.get("detail") or {}
        operation = _first_present(payload, detail, "operation")
        source = payload.get("source")

        try:
            # 通常の upload 完了要求と MediaConvert 状態通知を同じ Queue で処理する。
            if operation == "process-media-upload":
                _process_media_upload(payload, detail)
            elif source == "aws.mediaconvert":
                _process_mediaconvert_callback(payload, detail)
            else:
                logger.info(
                    "unsupported media event received",
                    extra={"source": source or "unknown", "event_id": payload.get("id", "unknown")},
                )
        except Exception:
            if not _mark_media_failed_on_final_attempt(record, payload, detail, operation):
                raise

        processed_count += 1

    return {"processed_count": processed_count}


def _parse_sqs_record(record: dict[str, Any]) -> dict[str, Any]:
    """SQS record body を JSON payload として取得する。"""
    body = record.get("body")
    if not isinstance(body, str):
        raise ValueError("SQS record body must be a string")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"failed to parse SQS record body: {error}") from error


def _process_media_upload(payload: dict[str, Any], detail: dict[str, Any]) -> None:
    """アップロード完了後の画像公開または動画変換を開始する。"""
    media_id = _required_string(_first_present(payload, detail, "mediaId"), "mediaId")
    resource_type = _required_int(_first_present(payload, detail, "resourceType"), "resourceType")
    upload_key = _required_string(_first_present(payload, detail, "uploadKey"), "uploadKey")
    public_key = _required_string(_first_present(payload, detail, "publicKey"), "publicKey")

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
    """MediaConvert の状態通知を DB の media_status に反映する。"""
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


def _first_present(primary: dict[str, Any], secondary: dict[str, Any], key: str) -> Any:
    """0 を有効値として扱うため、None の場合だけ fallback する。"""
    value = primary.get(key)
    if value is not None:
        return value

    return secondary.get(key)


def _mark_media_failed_on_final_attempt(
    record: dict[str, Any],
    payload: dict[str, Any],
    detail: dict[str, Any],
    operation: Any,
) -> bool:
    """SQS の最終受信時だけ media_status を failed に更新する。"""
    if operation != "process-media-upload" or not _is_final_receive_attempt(record):
        return False

    media_id = _required_string(_first_present(payload, detail, "mediaId"), "mediaId")
    logger.exception(
        "media upload processing failed on final receive attempt",
        extra={"media_id": media_id},
    )
    _mark_media_failed(media_id)
    return True


def _is_final_receive_attempt(record: dict[str, Any]) -> bool:
    """ApproximateReceiveCount が DLQ 移送前の上限に達したか判定する。"""
    max_receive_count = _optional_positive_int_env("EVENT_DLQ_MAX_RECEIVE_COUNT")
    if max_receive_count is None:
        return False

    attributes = record.get("attributes") or {}
    receive_count = _optional_positive_int(attributes.get("ApproximateReceiveCount"))
    return receive_count is not None and receive_count >= max_receive_count


def _publish_image(media_id: str, upload_key: str, public_key: str) -> None:
    """画像を uploads key から public key へコピーして published に更新する。"""
    bucket_name = _required_env("VIDEO_BUCKET_NAME")

    with _connect_db() as connection:
        media = _fetch_media_for_update(connection, media_id)
        if not _is_processable_media(media, RESOURCE_TYPE_IMAGE):
            return

        # CopyObject は同じ source/public key に収束するため、重複メッセージでも冪等に扱える。
        _s3().copy_object(
            Bucket=bucket_name,
            CopySource={"Bucket": bucket_name, "Key": upload_key},
            Key=public_key,
        )
        _update_media_status(connection, media_id, MEDIA_STATUS_PUBLISHED)
        connection.commit()


def _start_video_conversion(media_id: str, upload_key: str, public_key: str) -> None:
    """動画の MediaConvert job 作成権限を取得して変換を開始する。"""
    with _connect_db() as connection:
        media = _fetch_media_for_update(connection, media_id)
        if not _is_processable_media(media, RESOURCE_TYPE_VIDEO):
            return

        current_job_id = media.get("media_convert_job_id")
        if current_job_id and current_job_id != PENDING_JOB_ID:
            return
        if current_job_id == PENDING_JOB_ID:
            raise RuntimeError(f"MediaConvert job creation is already pending: {media_id}")

        # PENDING は二重 CreateJob を防ぐための処理権限マーカーとして使用する。
        acquired = _acquire_mediaconvert_job_creation(connection, media_id)
        if not acquired:
            raise RuntimeError(f"failed to acquire MediaConvert job creation: {media_id}")

        job_id = _create_mediaconvert_job(media_id, upload_key, public_key)
        _update_mediaconvert_job_id(connection, media_id, job_id)
        connection.commit()


def _fetch_media_for_update(connection: Any, media_id: str) -> dict[str, Any] | None:
    """対象 media を行ロック付きで取得する。"""
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
    """対象 media が指定種別かつ processing 状態か判定する。"""
    if media is None:
        return False

    return (
        media.get("resource_type") == resource_type
        and media.get("media_status") == MEDIA_STATUS_PROCESSING
    )


def _update_media_status(connection: Any, media_id: str, media_status: int) -> None:
    """processing 中の media_status を指定状態へ更新する。"""
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
    """MediaConvert job 作成権限を PENDING 更新で取得する。"""
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
    """PENDING 状態の media に実 MediaConvert job ID を保存する。"""
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
    """MediaConvert job ID に紐づく動画の media_status を更新する。"""
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


def _mark_media_failed(media_id: str) -> None:
    """processing 中の media を failed に更新する。"""
    with _connect_db() as connection:
        _update_media_status(connection, media_id, MEDIA_STATUS_FAILED)
        connection.commit()


def _create_mediaconvert_job(media_id: str, upload_key: str, public_key: str) -> str:
    """MediaConvert job を作成し、作成された job ID を返す。"""
    bucket_name = _required_env("VIDEO_BUCKET_NAME")
    role_arn = _required_env("MEDIACONVERT_ROLE_ARN")
    output_basename = _public_video_hls_basename(public_key)

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
                            "Destination": f"s3://{bucket_name}/{output_basename}",
                            "SegmentLength": 10,
                            "MinSegmentLength": 0,
                        },
                    },
                    "Outputs": [
                        {
                            "NameModifier": "_media",
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


def _public_video_hls_basename(public_key: str) -> str:
    """動画 publicKey から HLS 出力 base name を取得する。"""
    suffix = "/master.m3u8"
    if not public_key.endswith(suffix):
        raise ValueError(f"video publicKey must end with {suffix}: {public_key}")

    return public_key[: -len(".m3u8")]


def _connect_db() -> Any:
    """Secrets Manager の接続情報で Aurora MySQL に接続する。"""
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
    """Aurora 接続 Secret を読み込み、実行環境内でキャッシュする。"""
    global _db_secret

    if _db_secret is None:
        response = _secretsmanager().get_secret_value(SecretId=_required_env("DB_SECRET_ARN"))
        secret_string = _required_string(response.get("SecretString"), "SecretString")
        _db_secret = json.loads(secret_string)

    return _db_secret


def _s3() -> Any:
    """S3 client を遅延生成して再利用する。"""
    global _s3_client

    if _s3_client is None:
        import boto3

        _s3_client = boto3.client("s3")

    return _s3_client


def _mediaconvert() -> Any:
    """MediaConvert endpoint を解決して client を遅延生成する。"""
    global _mediaconvert_client
    global _mediaconvert_endpoint_url

    if _mediaconvert_client is None:
        import boto3

        if _mediaconvert_endpoint_url is None:
            _mediaconvert_endpoint_url = _discover_mediaconvert_endpoint(boto3.client("mediaconvert"))
        _mediaconvert_client = boto3.client("mediaconvert", endpoint_url=_mediaconvert_endpoint_url)

    return _mediaconvert_client


def _discover_mediaconvert_endpoint(client: Any) -> str:
    """MediaConvert の account 固有 endpoint URL を取得する。"""
    response = client.describe_endpoints(MaxResults=1)
    endpoints = response.get("Endpoints") or []
    if not endpoints:
        raise ValueError("MediaConvert endpoint was not returned")

    return _required_string(endpoints[0].get("Url"), "MediaConvert endpoint URL")


def _secretsmanager() -> Any:
    """Secrets Manager client を遅延生成して再利用する。"""
    global _secrets_client

    if _secrets_client is None:
        import boto3

        _secrets_client = boto3.client("secretsmanager")

    return _secrets_client


def _required_env(name: str) -> str:
    """必須環境変数を文字列として取得する。"""
    return _required_string(os.environ.get(name), name)


def _optional_positive_int_env(name: str) -> int | None:
    """任意環境変数を正の整数として取得する。"""
    return _optional_positive_int(os.environ.get(name))


def _optional_positive_int(value: Any) -> int | None:
    """値が設定されている場合だけ正の整数へ変換する。"""
    if value is None or value == "":
        return None

    parsed = _required_int(value, "positive integer")
    if parsed <= 0:
        return None

    return parsed


def _required_string(value: Any, name: str) -> str:
    """必須値が空でない文字列か検証する。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} is required")

    return value


def _required_int(value: Any, name: str) -> int:
    """必須値を整数として検証する。"""
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)

    raise ValueError(f"{name} must be an integer")
