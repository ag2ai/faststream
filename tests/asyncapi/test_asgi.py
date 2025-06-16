from faststream import FastStream
from faststream.asyncapi.generate import get_app_schema
from faststream.kafka import KafkaBroker


def test_asgi():
    schema = get_app_schema(FastStream(KafkaBroker())).to_jsonable()

    assert schema == {
        "asyncapi": "2.6.0",
        "defaultContentType": "application/json",
        "info": {"description": "", "title": "FastStream", "version": "0.1.0"},
        "servers": {
            "development": {
                "protocol": "kafka",
                "protocolVersion": "auto",
                "url": "localhost",
            }
        },
        "channels": {
            "ping": {
                "servers": "- development",
                "subscribe": {"bindings": {}, "tags": []},
            }
        },
        "components": {
            "messages": {},
            "schemas": {}
        },
    }
