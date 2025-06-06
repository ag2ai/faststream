import pytest

from faststream.rabbit.broker import RabbitRouter
from faststream.rabbit.fastapi import RabbitRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.rabbit()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = RabbitRouter
