import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event: dict[str, Any], context: Any) -> dict[str, str]:
    business_date = _resolve_business_date(event)
    mode = event.get("mode") or "DAILY"

    logger.info("action log batch started", extra={"business_date": business_date, "mode": mode})

    _run_task_a(business_date)
    _reflect_push_open_statistics(business_date)
    _run_task_b(business_date)
    _generate_delivery_tsv(business_date)
    _verify_delivery_tsv(business_date)
    _cleanup_intermediate_objects(business_date)

    return {"business_date": business_date, "mode": mode, "status": "SUCCEEDED"}


def _resolve_business_date(event: dict[str, Any]) -> str:
    value = event.get("business_date")
    if isinstance(value, str) and value:
        return value

    return (datetime.now(UTC).date() - timedelta(days=1)).isoformat()


def _run_task_a(business_date: str) -> None:
    logger.info("task a prepared", extra={"business_date": business_date})


def _reflect_push_open_statistics(business_date: str) -> None:
    logger.info("push open statistics reflection prepared", extra={"business_date": business_date})


def _run_task_b(business_date: str) -> None:
    logger.info("task b prepared", extra={"business_date": business_date})


def _generate_delivery_tsv(business_date: str) -> None:
    logger.info("delivery tsv generation prepared", extra={"business_date": business_date})


def _verify_delivery_tsv(business_date: str) -> None:
    logger.info("delivery tsv verification prepared", extra={"business_date": business_date})


def _cleanup_intermediate_objects(business_date: str) -> None:
    logger.info("intermediate cleanup prepared", extra={"business_date": business_date})
