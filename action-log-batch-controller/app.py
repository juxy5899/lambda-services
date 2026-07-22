from action_log_batch_controller.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
