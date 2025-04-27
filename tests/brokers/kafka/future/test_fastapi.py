import pytest

from faststream.kafka.fastapi import KafkaRouter as StreamRouter
from faststream.kafka.router import KafkaRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.kafka()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
