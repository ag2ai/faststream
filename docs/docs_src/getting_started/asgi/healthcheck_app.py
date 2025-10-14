from faststream.nats import NatsBroker
from faststream.asgi import AsgiFastStream, make_ping_asgi

broker = NatsBroker()

app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/health", make_ping_asgi(broker, timeout=5.0)),
    ]
)
