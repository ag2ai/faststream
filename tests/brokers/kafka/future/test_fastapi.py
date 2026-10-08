import pytest

from faststream.kafka.broker import KafkaRouter
from faststream.kafka.fastapi import KafkaRouter as StreamRouter
from tests.brokers.base.future.fastapi import FastapiTestCase
from tests.brokers.kafka.basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestRouter(KafkaTestcaseConfig, FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
