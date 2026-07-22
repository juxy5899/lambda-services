import json
import logging
from collections import defaultdict
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int]:
    records = event.get("Records") or []
    messages = [_parse_sqs_record(record) for record in records]
    aggregated = _aggregate_by_execution(messages)

    for execution_id, summary in aggregated.items():
        _update_execution_summary(execution_id, summary)

    return {"processed_count": len(messages), "execution_count": len(aggregated)}


def _parse_sqs_record(record: dict[str, Any]) -> dict[str, Any]:
    body = record.get("body")
    if not isinstance(body, str):
        raise ValueError("SQS record body must be a string")

    try:
        return json.loads(body)
    except json.JSONDecodeError as error:
        raise ValueError(f"failed to parse SQS record body: {error}") from error


def _aggregate_by_execution(messages: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "success_count": 0,
            "fail_count": 0,
            "skipped_count": 0,
            "invalid_endpoint_ids": set(),
        }
    )

    for message in messages:
        execution_id = message.get("execution_id")
        if not execution_id:
            raise ValueError("execution_id is required")

        summary = summaries[execution_id]
        summary["success_count"] += int(message.get("success_count") or 0)
        summary["fail_count"] += int(message.get("fail_count") or 0)
        summary["skipped_count"] += int(message.get("skipped_count") or 0)
        summary["invalid_endpoint_ids"].update(message.get("invalid_endpoint_ids") or [])

    return dict(summaries)


def _update_execution_summary(execution_id: str, summary: dict[str, Any]) -> None:
    logger.info(
        "execution summary prepared",
        extra={
            "execution_id": execution_id,
            "success_count": summary["success_count"],
            "fail_count": summary["fail_count"],
            "skipped_count": summary["skipped_count"],
            "invalid_endpoint_count": len(summary["invalid_endpoint_ids"]),
        },
    )
