from faststream.__about__ import __version__


class MessageAction:
    CREATE = "create"
    PUBLISH = "publish"
    PROCESS = "process"
    RECEIVE = "receive"


OTEL_SCHEMA = "https://opentelemetry.io/schemas/1.11.0"
ERROR_TYPE = "error.type"
MESSAGING_DESTINATION_PUBLISH_NAME = "messaging.destination_publish.name"

# NOTE: the emitted string comes from the deprecated `SpanAttributes` enum. Modern
# semantic conventions renamed it to `messaging.message.body.size`
# (`MESSAGING_MESSAGE_BODY_SIZE`), but adopting that constant would silently
# rename an attribute users already have in their dashboards and alerts, so the
# original string is kept here until the rename is scheduled deliberately.
MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES = "messaging.message.payload_size_bytes"

WITH_BATCH = "with_batch"
INSTRUMENTING_MODULE_NAME = "opentelemetry.instrumentation.faststream"
INSTRUMENTING_LIBRARY_VERSION = __version__
