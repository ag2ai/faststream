from typing import Any

import pytest
from syrupy.assertion import SnapshotAssertion

from faststream.redis import RedisBroker, RedisPublisher, RedisRoute, RedisRouter
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


@pytest.mark.redis()
class TestRouter(RouterTestcase):
    broker_class = RedisBroker
    router_class = RedisRouter
    route_class = RedisRoute
    publisher_class = RedisPublisher

    def test_prefix(self, snapshot_json: SnapshotAssertion) -> None:
        broker = self.broker_class()

        router = self.router_class(prefix="test_")

        @router.subscriber("test")
        async def handle(msg: Any) -> None: ...

        broker.include_router(router)

        schema = self.get_spec(broker).to_jsonable()

        assert schema == snapshot_json


@pytest.mark.redis()
class TestRouterArguments(ArgumentsTestcase):
    broker_class = RedisRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(RedisBroker(routers=broker))


@pytest.mark.redis()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RedisRouter

    def get_spec(self, *broker: Any) -> Specification:
        return super().get_spec(RedisBroker(routers=broker))
