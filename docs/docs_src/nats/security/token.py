import os

from faststream.nats import NatsBroker, NatsToken


def load_token() -> str:
    return os.environ["NATS_TOKEN"]


broker = NatsBroker(security=NatsToken(load_token))
