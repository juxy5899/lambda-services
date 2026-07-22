from push_aggregator.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
