from action_log_batch_controller.handler import handle_event


def test_handle_event_uses_business_date():
    result = handle_event({"business_date": "2026-10-01", "mode": "BACKFILL"}, None)

    assert result == {"business_date": "2026-10-01", "mode": "BACKFILL", "status": "SUCCEEDED"}
