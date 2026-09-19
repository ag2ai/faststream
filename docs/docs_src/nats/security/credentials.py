from faststream.nats import NatsBroker, NatsCredentials

broker = NatsBroker(
    security=NatsCredentials.from_file("/run/secrets/nats/user.creds"),
)
