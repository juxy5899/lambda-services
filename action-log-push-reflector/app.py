from action_log_push_reflector.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
