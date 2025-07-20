import pytest

from faststream.kafka import (
    KafkaPublisher,
    KafkaRoute,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
class TestRouter(KafkaTestcaseConfig, RouterTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher


@pytest.mark.kafka()
@pytest.mark.connected()
class TestRouterLocal(KafkaMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher
