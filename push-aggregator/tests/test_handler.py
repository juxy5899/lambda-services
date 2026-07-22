from push_aggregator.handler import handle_event


def test_handle_event_accepts_empty_records():
    result = handle_event({"Records": []}, None)

    assert result == {"processed_count": 0, "execution_count": 0}
