from faststream.nats import NatsBroker, NatsNKey

broker = NatsBroker(
    security=NatsNKey.from_file("/run/secrets/nats/user.nk"),
)
