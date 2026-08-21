import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# 送信種別（t_notification.send_type と同一のコード値）
SEND_TYPE_PUSH = 0

# End User Messaging（Pinpoint）の配信結果ステータス
DELIVERY_STATUS_SUCCESSFUL = "SUCCESSFUL"
DELIVERY_STATUS_OPT_OUT = "OPT_OUT"
DELIVERY_STATUS_DUPLICATE = "DUPLICATE"
DELIVERY_STATUS_PERMANENT_FAILURE = "PERMANENT_FAILURE"

# 端末無効を示すエラー文言。該当する endpoint_id は Aggregator へ無効端末として報告する。
INVALID_ENDPOINT_KEYWORDS = (
    "tokeninvalid",
    "invalidtoken",
    "invalid token",
    "deviceunregistered",
    "device unregistered",
    "notregistered",
    "not registered",
    "unregistered",
    "invalidregistration",
    "expiredtoken",
    "endpoint is inactive",
    "endpoint disabled",
)

# End User Messaging 呼び出しの再試行回数と初期待機秒数
MAX_SEND_ATTEMPTS = 3
RETRY_BASE_WAIT_SECONDS = 0.5

# エラー概要として Aggregator へ渡すサンプル数の上限
MAX_ERROR_SAMPLES = 5

_pinpoint_client = None
_sqs_client = None


def handle_event(event: dict[str, Any], context: Any) -> dict[str, list[dict[str, str]]]:
    """Worker SQS から受信した配信チャンクを Push 送信し、結果を Aggregator SQS へ送信する。

    1 メッセージ = 1 チャンクとして処理し、処理できなかったメッセージだけを
    ``batchItemFailures`` として報告することで、成功済みチャンクの再配送を避ける。
    """
    records = event.get("Records") or []
    batch_item_failures: list[dict[str, str]] = []

    for record in records:
        message_id = record.get("messageId", "unknown")

        try:
            chunk = _parse_sqs_record(record)
            result = _process_chunk(chunk)
            _send_chunk_result(chunk, result)
        except Exception:
            # 一時的な AWS エラーは SQS の標準再試行に委ね、上限超過分は DLQ で検知する
            logger.exception("push chunk processing failed", extra={"message_id": message_id})
            batch_item_failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": batch_item_failures}


def _parse_sqs_record(record: dict[str, Any]) -> dict[str, Any]:
    """SQS record body を配信チャンクの JSON として取得する。"""
    body = record.get("body")
    if not isinstance(body, str):
        raise ValueError("SQS record body must be a string")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"failed to parse SQS record body: {error}") from error


def _process_chunk(chunk: dict[str, Any]) -> dict[str, Any]:
    """チャンク内の全対象へ Push 送信し、件数と無効端末を集計する。"""
    execution_id = _required_string(chunk.get("execution_id"), "execution_id")
    chunk_id = _required_string(chunk.get("chunk_id"), "chunk_id")
    notification_id = _required_value(chunk.get("notification_id"), "notification_id")
    send_type = _required_value(chunk.get("send_type"), "send_type")
    targets = chunk.get("targets") or []

    result: dict[str, Any] = {
        "success_count": 0,
        "fail_count": 0,
        "skipped_count": 0,
        "invalid_endpoint_ids": [],
        "error_samples": [],
    }

    # お知らせのみ（send_type=1）は Spring Boot Dispatch が完結させるため Worker では送信しない
    if int(send_type) != SEND_TYPE_PUSH:
        logger.warning(
            "non push chunk received on worker queue",
            extra={
                "execution_id": execution_id,
                "chunk_id": chunk_id,
                "send_type": int(send_type),
            },
        )
        result["skipped_count"] = len(targets)
        result["error_samples"] = ["UnsupportedSendType"]
        return result

    # endpoint_id を持たない対象はスキップとして計上し、送信対象から除外する
    endpoint_ids: list[str] = []
    for target in targets:
        endpoint_id = target.get("endpoint_id")
        if isinstance(endpoint_id, str) and endpoint_id:
            endpoint_ids.append(endpoint_id)
        else:
            result["skipped_count"] += 1

    if not endpoint_ids:
        logger.info(
            "push chunk has no sendable endpoint",
            extra={"execution_id": execution_id, "chunk_id": chunk_id},
        )
        return result

    message_configuration = _build_message_configuration(chunk, notification_id, execution_id)
    application_id = _required_env("PUSH_APPLICATION_ID")
    send_batch_size = _positive_int_env("SEND_BATCH_SIZE", 100)
    invalid_endpoint_ids: set[str] = set()
    error_samples: list[str] = []

    for batch in _iter_batches(endpoint_ids, send_batch_size):
        endpoint_results = _send_messages(application_id, batch, message_configuration, execution_id)

        for endpoint_id in batch:
            endpoint_result = endpoint_results.get(endpoint_id) or {}
            status = str(endpoint_result.get("DeliveryStatus") or "UNKNOWN_FAILURE")

            if status == DELIVERY_STATUS_SUCCESSFUL:
                result["success_count"] += 1
                continue

            if status in (DELIVERY_STATUS_OPT_OUT, DELIVERY_STATUS_DUPLICATE):
                result["skipped_count"] += 1
                _append_error_sample(error_samples, status)
                continue

            result["fail_count"] += 1
            status_message = str(endpoint_result.get("StatusMessage") or "")
            _append_error_sample(error_samples, f"{status}:{status_message}"[:200] or status)

            if _is_invalid_endpoint(status, status_message):
                invalid_endpoint_ids.add(endpoint_id)

    result["invalid_endpoint_ids"] = sorted(invalid_endpoint_ids)
    result["error_samples"] = error_samples

    logger.info(
        "push chunk processed",
        extra={
            "execution_id": execution_id,
            "chunk_id": chunk_id,
            "notification_id": notification_id,
            "success_count": result["success_count"],
            "fail_count": result["fail_count"],
            "skipped_count": result["skipped_count"],
            "invalid_endpoint_count": len(result["invalid_endpoint_ids"]),
        },
    )

    return result


def _build_message_configuration(
    chunk: dict[str, Any],
    notification_id: Any,
    execution_id: str,
) -> dict[str, Any]:
    """iOS / Android 双方へ同一内容を配信するメッセージ設定を組み立てる。

    カスタムデータには開封ログの帰属に必要な notification_id と execution_id を必ず含める。
    """
    notification = chunk.get("notification") or {}
    title = _required_string(notification.get("title"), "notification.title")
    body = _required_string(notification.get("body"), "notification.body")
    image_url = notification.get("image_url")
    redirect_url = notification.get("redirect_url")

    data = {
        "notification_id": str(notification_id),
        "execution_id": execution_id,
    }
    if isinstance(redirect_url, str) and redirect_url:
        data["redirect_url"] = redirect_url

    apns_message: dict[str, Any] = {
        "Action": "OPEN_APP",
        "Title": title,
        "Body": body,
        "SilentPush": False,
        "Data": data,
    }
    gcm_message: dict[str, Any] = {
        "Action": "OPEN_APP",
        "Title": title,
        "Body": body,
        "SilentPush": False,
        "Data": data,
    }

    if isinstance(image_url, str) and image_url:
        apns_message["MediaUrl"] = image_url
        gcm_message["ImageUrl"] = image_url

    return {
        "APNSMessage": apns_message,
        "GCMMessage": gcm_message,
        "DefaultPushNotificationMessage": {
            "Action": "OPEN_APP",
            "Title": title,
            "Body": body,
            "SilentPush": False,
            "Data": data,
        },
    }


def _send_messages(
    application_id: str,
    endpoint_ids: list[str],
    message_configuration: dict[str, Any],
    execution_id: str,
) -> dict[str, Any]:
    """End User Messaging へバッチ送信し、endpoint_id ごとの結果を返す。

    Throttling 等の一時的な失敗は指数バックオフで再試行し、上限超過時は例外を送出して
    SQS の再配送に委ねる。
    """
    request = {
        "ApplicationId": application_id,
        "MessageRequest": {
            "Endpoints": {endpoint_id: {} for endpoint_id in endpoint_ids},
            "MessageConfiguration": message_configuration,
            "TraceId": execution_id,
        },
    }

    last_error: Exception | None = None
    for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
        try:
            response = _pinpoint().send_messages(**request)
            message_response = response.get("MessageResponse") or {}
            return message_response.get("EndpointResult") or {}
        except Exception as error:  # noqa: BLE001 - SDK 例外を一律に再試行対象とする
            last_error = error
            logger.warning(
                "end user messaging send attempt failed",
                extra={
                    "execution_id": execution_id,
                    "attempt": attempt,
                    "endpoint_count": len(endpoint_ids),
                },
            )
            if attempt < MAX_SEND_ATTEMPTS:
                time.sleep(RETRY_BASE_WAIT_SECONDS * (2 ** (attempt - 1)))

    raise RuntimeError("end user messaging send failed after retries") from last_error


def _send_chunk_result(chunk: dict[str, Any], result: dict[str, Any]) -> None:
    """チャンク単位の集計結果を Aggregator SQS へ送信する。"""
    message = {
        "execution_id": chunk.get("execution_id"),
        "chunk_id": chunk.get("chunk_id"),
        "success_count": result["success_count"],
        "fail_count": result["fail_count"],
        "skipped_count": result["skipped_count"],
        "invalid_endpoint_ids": result["invalid_endpoint_ids"],
        "error_samples": result["error_samples"],
    }

    _sqs().send_message(
        QueueUrl=_required_env("AGGREGATOR_QUEUE_URL"),
        MessageBody=json.dumps(message, ensure_ascii=False),
    )


def _is_invalid_endpoint(status: str, status_message: str) -> bool:
    """端末無効（トークン失効・アンインストール等）を示す結果かを判定する。"""
    normalized = status_message.lower()
    if any(keyword in normalized for keyword in INVALID_ENDPOINT_KEYWORDS):
        return True

    return status == DELIVERY_STATUS_PERMANENT_FAILURE and "endpoint" in normalized


def _append_error_sample(error_samples: list[str], sample: str) -> None:
    """エラー概要のサンプルを重複なく上限件数まで保持する。"""
    if sample and sample not in error_samples and len(error_samples) < MAX_ERROR_SAMPLES:
        error_samples.append(sample)


def _iter_batches(values: list[str], batch_size: int):
    """リストを指定件数ごとに分割する。"""
    for index in range(0, len(values), batch_size):
        yield values[index : index + batch_size]


def _pinpoint() -> Any:
    """End User Messaging（Pinpoint）client を遅延生成して再利用する。"""
    global _pinpoint_client

    if _pinpoint_client is None:
        import boto3

        _pinpoint_client = boto3.client("pinpoint")

    return _pinpoint_client


def _sqs() -> Any:
    """SQS client を遅延生成して再利用する。"""
    global _sqs_client

    if _sqs_client is None:
        import boto3

        _sqs_client = boto3.client("sqs")

    return _sqs_client


def _required_env(name: str) -> str:
    """必須の環境変数を取得する。"""
    value = os.environ.get(name)
    if not value:
        raise ValueError(f"environment variable is required: {name}")

    return value


def _positive_int_env(name: str, default: int) -> int:
    """正の整数の環境変数を取得する。不正値の場合は既定値を使用する。"""
    raw = os.environ.get(name)
    if raw is None:
        return default

    try:
        value = int(raw)
    except ValueError:
        return default

    return value if value > 0 else default


def _required_string(value: Any, name: str) -> str:
    """必須の文字列項目を取得する。"""
    if not isinstance(value, str) or not value:
        raise ValueError(f"field is required: {name}")

    return value


def _required_value(value: Any, name: str) -> Any:
    """0 を有効値として扱う必須項目を取得する。"""
    if value is None:
        raise ValueError(f"field is required: {name}")

    return value
