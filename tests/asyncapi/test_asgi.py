from faststream.asgi import AsgiFastStream, get, make_ping_asgi
from faststream.kafka import KafkaBroker
from faststream.specification import AsyncAPI
from faststream.specification.asyncapi.v2_6_0 import (
    get_app_schema as get_app_schema_v2_6_0
)
from faststream.specification.asyncapi.v3_0_0 import (
    get_app_schema as get_app_schema_v3_0_0
)


def test_asgi_v2_6_0() -> None:
    broker = KafkaBroker()

    @get
    async def handler() -> None: ...

    @get(include_in_schema=False)
    async def handler2() -> None: ...

    app = AsgiFastStream(
        broker,
        asgi_routes=[
            (
                "/test",
                make_ping_asgi(
                    broker,
                    description="test description",
                    tags=[{"name": "test"}],
                ),
            ),
            ("/test2", handler),
            ("/test3", handler2),
        ],
        specification=AsyncAPI(schema_version="2.6.0")
    )

    schema = app.schema.to_specification().to_jsonable()

    assert schema["channels"] == {
        "/test": {
            "description": "test description",
            "subscribe": {
                "bindings": {
                    "http": {"method": "GET, HEAD", "bindingVersion": "0.1.0"}
                },
                "tags": [{"name": "test"}],
            },
        },
        "/test2": {
            "subscribe": {
                "bindings": {
                    "http": {"method": "GET, HEAD", "bindingVersion": "0.1.0"}
                }
            }
        },
    }


def test_asgi_v3_0_0() -> None:
    broker = KafkaBroker()

    @get
    async def handler() -> None: ...

    @get(include_in_schema=False)
    async def handler2() -> None: ...

    app = AsgiFastStream(
        broker,
        asgi_routes=[
            (
                "/test",
                make_ping_asgi(
                    broker,
                    description="test description",
                    tags=[{"name": "test"}],
                ),
            ),
            ("/test2", handler),
            ("/test3", handler2),
        ],
        specification=AsyncAPI(schema_version="3.0.0")
    )

    schema = app.schema.to_specification().to_jsonable()

    assert schema["channels"] == {
        "testHttpChannel": {
            "address": "/test",
            "description": "test description",
            "messages": {}
        },
        "test2HttpChannel": {
            "address": "/test2",
            "messages": {}
        }
    }
    assert schema["operations"] == {
        "testHttpChannel": {
            "action": "receive",
            "channel": {
                "$ref": "#/channels/testHttpChannel"
            },
            "bindings": {
                "http": {
                    "method": "GET",
                    "bindingVersion": "0.3.0"
                }
            },
            "messages": []
        },
        "test2HttpChannel": {
            "action": "receive",
            "channel": {
                "$ref": "#/channels/test2HttpChannel"
            },
            "bindings": {
                "http": {
                    "method": "GET",
                    "bindingVersion": "0.3.0"
                }
            },
            "messages": []
        }
    }
