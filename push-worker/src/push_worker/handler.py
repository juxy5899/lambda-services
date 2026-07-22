import json
import logging
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int]:
    records = event.get("Records") or []
    processed_count = 0

    for record in records:
        message = _parse_sqs_record(record)
        result = _process_chunk(message)
        _send_aggregator_result(result)
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


def _process_chunk(message: dict[str, Any]) -> dict[str, Any]:
    targets = message.get("targets") or []
    send_type = message.get("send_type")

    logger.info(
        "push worker chunk received",
        extra={
            "execution_id": message.get("execution_id"),
            "send_type": send_type,
            "target_count": len(targets),
        },
    )

    return {
        "execution_id": message.get("execution_id"),
        "success_count": len(targets),
        "fail_count": 0,
        "skipped_count": 0,
        "invalid_endpoint_ids": [],
        "error_samples": [],
    }


def _send_aggregator_result(result: dict[str, Any]) -> None:
    logger.info("aggregator result prepared", extra={"execution_id": result.get("execution_id")})
