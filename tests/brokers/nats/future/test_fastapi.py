import pytest

from faststream.nats.broker.router import NatsRouter
from faststream.nats.fastapi import NatsRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.connected()
@pytest.mark.nats()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = NatsRouter
