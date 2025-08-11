from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.redis import TestRedisBroker
from faststream.redis.fastapi import RedisRouter
from faststream.specification import Specification
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


@pytest.mark.redis()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = RedisRouter
    router_class = RedisRouter
    broker_wrapper = staticmethod(TestRedisBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.redis()
class TestRouterPublisher(PublisherTestcase):
    broker_class = RedisRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)
