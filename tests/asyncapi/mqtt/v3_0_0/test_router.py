from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.mqtt import MQTTBroker, MQTTPublisher, MQTTRoute, MQTTRouter
from faststream.specification.base import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v3_0_0.router import RouterTestcase


@pytest.mark.mqtt()
class TestRouter(RouterTestcase):
    broker_class = MQTTBroker
    router_class = MQTTRouter
    route_class = MQTTRoute
    publisher_class = MQTTPublisher

    def test_prefix(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg: Any) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json


@pytest.mark.mqtt()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = MQTTRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(MQTTBroker(routers=broker))


@pytest.mark.mqtt()
class TestRouterPublisher(PublisherTestcase):
    broker_class = MQTTRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(MQTTBroker(routers=broker))
