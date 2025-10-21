from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.rabbit.fastapi import RabbitRouter
from faststream.rabbit.testing import TestRabbitBroker
from faststream.security import SASLPlaintext
from faststream.specification import Specification
from tests.asyncapi.base.v3_0_0 import get_3_0_0_schema
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.rabbit()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = RabbitRouter
    router_class = RabbitRouter
    broker_wrapper = staticmethod(TestRabbitBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.rabbit()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RabbitRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.rabbit()
def test_fastapi_security_schema() -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = RabbitRouter(security=security)

    schema = get_3_0_0_schema(router.broker)

    assert schema["servers"]["development"] == {
        "protocol": "amqp",
        "protocolVersion": "0.9.1",
        "security": [{"$ref": "#/components/securitySchemes/user-password"}],
        "host": "user:pass@localhost:5672",
        "pathname": "/",
    }
    assert schema["components"]["securitySchemes"] == {
        "user-password": {"type": "userPassword"},
    }
