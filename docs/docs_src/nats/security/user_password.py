from faststream.nats import NatsBroker, NatsUserPassword

broker = NatsBroker(
    security=NatsUserPassword(
        username="user",
        password="password",
    ),
)
