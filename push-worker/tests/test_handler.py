from push_worker.handler import handle_event


def test_handle_event_accepts_empty_records():
    result = handle_event({"Records": []}, None)

    assert result == {"processed_count": 0}
