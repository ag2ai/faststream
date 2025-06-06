import pytest

from faststream.kafka.broker import KafkaRouter
from faststream.kafka.fastapi import KafkaRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase


@pytest.mark.kafka()
class TestRouter(FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
