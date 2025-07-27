from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.confluent.fastapi import KafkaRouter
from faststream.confluent.testing import TestKafkaBroker
from faststream.security import SASLPlaintext
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0 import get_2_6_0_schema
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.confluent()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = KafkaRouter
    router_class = KafkaRouter
    broker_wrapper = staticmethod(TestKafkaBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.confluent()
class TestRouterPublisher(PublisherTestcase):
    broker_class = KafkaRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.confluent()
def test_fastapi_security_schema() -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = KafkaRouter("localhost:9092", security=security)

    schema = get_2_6_0_schema(router.broker)

    assert schema["servers"]["development"] == {
        "protocol": "kafka",
        "protocolVersion": "auto",
        "security": [{"user-password": []}],
        "url": "localhost:9092",
    }
    assert schema["components"]["securitySchemes"] == {
        "user-password": {"type": "userPassword"},
    }
