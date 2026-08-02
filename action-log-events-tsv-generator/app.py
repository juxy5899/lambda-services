from action_log_events_tsv_generator.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
