from push_worker.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
