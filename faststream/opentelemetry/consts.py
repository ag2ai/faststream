from faststream.__about__ import __version__


class MessageAction:
    CREATE = "create"
    PUBLISH = "publish"
    PROCESS = "process"
    RECEIVE = "receive"


OTEL_SCHEMA = "https://opentelemetry.io/schemas/1.11.0"
ERROR_TYPE = "error.type"
MESSAGING_DESTINATION_PUBLISH_NAME = "messaging.destination_publish.name"
WITH_BATCH = "with_batch"
INSTRUMENTING_MODULE_NAME = "opentelemetry.instrumentation.faststream"
INSTRUMENTING_LIBRARY_VERSION = __version__
