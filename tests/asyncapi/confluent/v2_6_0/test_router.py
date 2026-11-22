from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.confluent import KafkaBroker, KafkaPublisher, KafkaRoute, KafkaRouter
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


@pytest.mark.confluent()
class TestRouter(RouterTestcase):
    broker_class = KafkaBroker
    router_class = KafkaRouter
    route_class = KafkaRoute
    publisher_class = KafkaPublisher

    def test_prefix(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg: Any) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json


@pytest.mark.confluent()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = KafkaRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(KafkaBroker(routers=broker))


@pytest.mark.confluent()
class TestRouterPublisher(PublisherTestcase):
    broker_class = KafkaRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(KafkaBroker(routers=broker))
