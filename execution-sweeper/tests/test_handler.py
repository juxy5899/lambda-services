from execution_sweeper.handler import handle_event


def test_handle_event_returns_closed_count():
    result = handle_event({}, None)

    assert result == {"closed_count": 0}
