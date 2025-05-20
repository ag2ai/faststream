
from faststream.asgi import AsgiFastStream, AsgiResponse, get, make_ping_asgi
from faststream.asgi.types import Scope
from faststream.asyncapi.generate import get_app_schema


def test_asyncapi_generate() -> None:
    from faststream.kafka import KafkaBroker

    broker = KafkaBroker()

    @get
    async def liveness_ping(scope: Scope) -> AsgiResponse:
        """Liveness ping."""
        return AsgiResponse(b"", status_code=200)

    routes = [
        ("/liveness", liveness_ping),
        ("/raw", AsgiResponse(b"", status_code=200)),  # should not be rendered
        ("/readiness", make_ping_asgi(broker, timeout=5.0, include_in_schema=False)),
    ]

    schema = get_app_schema(
        AsgiFastStream(broker, asgi_routes=routes)
    ).to_jsonable()

    assert schema["routes"] == [{
        "path": "/liveness",
        "methods": ["GET", "HEAD"],
        "description": "Liveness ping.",
    }]

