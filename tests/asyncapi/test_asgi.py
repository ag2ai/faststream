import pytest

from faststream.asgi import AsgiFastStream, get, make_ping_asgi
from faststream.kafka import KafkaBroker
from faststream.specification.asyncapi.v2_6_0 import get_app_schema


@pytest.mark.xfail(
    reason="We still don't know how to pass asgi routes to AsyncAPI specification object"
)
def test_asgi() -> None:
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
    )

    schema = get_app_schema(app).to_jsonable()

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
                "bindings": {"http": {"method": "GET, HEAD", "bindingVersion": "0.1.0"}}
            }
        },
    }
