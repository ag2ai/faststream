from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.kafka.fastapi import KafkaRouter
from faststream.kafka.testing import TestKafkaBroker
from faststream.security import SASLPlaintext
from faststream.specification import Specification
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.kafka()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = KafkaRouter
    router_class = KafkaRouter
    broker_wrapper = staticmethod(TestKafkaBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.kafka()
class TestRouterPublisher(PublisherTestcase):
    broker_class = KafkaRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.kafka()
def test_fastapi_security_schema() -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = KafkaRouter("localhost:9092", security=security)

    schema = get_3_0_0_schema(router.broker)

    assert schema["servers"]["development"] == {
        "protocol": "kafka",
        "protocolVersion": "auto",
        "security": [{"$ref": "#/components/securitySchemes/user-password"}],
        "host": "localhost:9092",
        "pathname": "",
    }
    assert schema["components"]["securitySchemes"] == {
        "user-password": {"type": "userPassword"},
    }
