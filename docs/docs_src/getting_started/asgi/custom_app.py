from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.asgi.types import Scope

broker = NatsBroker()

@get
async def liveness_ping(scope: Scope) -> AsgiResponse:
    return AsgiResponse(b"", status_code=200)

app = AsgiFastStream(
    broker,
    asgi_routes=[("/health", liveness_ping)],
)
