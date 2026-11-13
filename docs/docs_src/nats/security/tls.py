from ssl import create_default_context

from faststream.nats import NatsBroker, NatsSecurity

ssl_context = create_default_context()

broker = NatsBroker(
    "tls://nats.example.com:4222",
    security=NatsSecurity(
        ssl_context=ssl_context,
        tls_hostname="nats.example.com",
        tls_handshake_first=True,
    ),
)
