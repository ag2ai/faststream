import pytest

from faststream.kafka import (
    KafkaPublisher,
    KafkaRoute,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import KafkaMemoryTestcaseConfig, KafkaTestcaseConfig


@pytest.mark.kafka()
class TestRouter(KafkaTestcaseConfig, RouterTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher


class TestRouterLocal(KafkaMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher
