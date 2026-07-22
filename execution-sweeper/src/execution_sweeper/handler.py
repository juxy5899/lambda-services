import logging
import os
from typing import Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def handle_event(event: dict[str, Any], context: Any) -> dict[str, int]:
    stale_minutes = int(os.environ.get("STALE_EXECUTION_MINUTES", "60"))
    stale_executions = _find_stale_executions(stale_minutes)

    for execution in stale_executions:
        _close_execution(execution)

    return {"closed_count": len(stale_executions)}


def _find_stale_executions(stale_minutes: int) -> list[dict[str, Any]]:
    logger.info("stale execution scan requested", extra={"stale_minutes": stale_minutes})
    return []


def _close_execution(execution: dict[str, Any]) -> None:
    logger.info("stale execution close prepared", extra={"execution_id": execution.get("id")})
