import pytest

from faststream.confluent.broker.router import KafkaRouter
from faststream.confluent.fastapi import KafkaRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase
from tests.brokers.confluent.basic import ConfluentTestcaseConfig


@pytest.mark.connected()
@pytest.mark.confluent()
class TestRouter(ConfluentTestcaseConfig, FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
