from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

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

    def get_spec(self, *routers: Any) -> Specification:
        return super().get_spec(*(router.broker for router in routers))


@pytest.mark.rabbit()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RabbitRouter

    def get_spec(self, *routers: Any) -> Specification:
        return super().get_spec(*(router.broker for router in routers))


@pytest.mark.rabbit()
def test_fastapi_security_schema(snapshot_json: SnapshotAssertion) -> None:
    security = SASLPlaintext(username="user", password="pass", use_ssl=False)

    router = RabbitRouter(security=security)

    schema = get_3_0_0_schema(router.broker)

    assert schema == snapshot_json
