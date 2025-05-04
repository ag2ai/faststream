import pytest

from faststream.rabbit.fastapi import RabbitRouter as StreamRouter
from faststream.rabbit.router import RabbitRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.rabbit()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = RabbitRouter
