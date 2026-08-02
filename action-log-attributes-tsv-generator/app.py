from action_log_attributes_tsv_generator.handler import handle_event


def handler(event, context):
    return handle_event(event, context)
