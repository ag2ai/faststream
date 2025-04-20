import pytest

from faststream.confluent.fastapi import KafkaRouter as StreamRouter
from faststream.confluent.router import KafkaRouter
from tests.brokers.base.future.fastapi import FastapiTestCase
from tests.brokers.confluent.basic import ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestRouter(ConfluentTestcaseConfig, FastapiTestCase):
    router_class = StreamRouter
    broker_router_class = KafkaRouter
