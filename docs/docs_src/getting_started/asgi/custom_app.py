from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, AsgiResponse, get

broker = NatsBroker()

@get
async def liveness_ping(scope):
    return AsgiResponse(b"", status_code=200)

app = AsgiFastStream(
    broker,
    asgi_routes=[("/health", liveness_ping)],
)
