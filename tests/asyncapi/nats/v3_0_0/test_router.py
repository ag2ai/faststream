from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.nats import NatsBroker, NatsPublisher, NatsRoute, NatsRouter
from faststream.specification.base import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v3_0_0.router import RouterTestcase


@pytest.mark.nats()
class TestRouter(RouterTestcase):
    broker_class = NatsBroker
    router_class = NatsRouter
    route_class = NatsRoute
    publisher_class = NatsPublisher

    def test_prefix(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg: Any) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json


@pytest.mark.nats()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = NatsRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(NatsBroker(routers=broker))


@pytest.mark.nats()
class TestRouterPublisher(PublisherTestcase):
    broker_class = NatsRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(NatsBroker(routers=broker))
