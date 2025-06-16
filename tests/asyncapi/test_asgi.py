from faststream.asgi import AsgiFastStream, get, make_ping_asgi
from faststream.asyncapi.generate import get_app_schema
from faststream.kafka import KafkaBroker


def test_asgi():
    broker = KafkaBroker()

    @get
    async def handler(): ...

    @get(include_in_schema=False)
    async def handler2(): ...

    app = AsgiFastStream(
        broker,
        asgi_routes=[
            (
                "/test",
                make_ping_asgi(
                    broker,
                    description="test desctiption",
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
            "description": "test desctiption",
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
