import pytest

from faststream.confluent import (
    KafkaPublisher,
    KafkaRoute,
)
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import ConfluentMemoryTestcaseConfig, ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestRouter(ConfluentTestcaseConfig, RouterTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher


class TestRouterLocal(ConfluentMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = KafkaRoute
    publisher_class = KafkaPublisher
