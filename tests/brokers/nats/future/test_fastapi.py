import pytest

from faststream.nats.fastapi import NatsRouter as StreamRouter
from faststream.nats.router import NatsRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.nats()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = NatsRouter
