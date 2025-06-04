from faststream.redis import TestRedisBroker
from faststream.redis.fastapi import RedisRouter
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = staticmethod(lambda: RedisRouter().broker)
    router_class = RedisRouter
    broker_wrapper = staticmethod(TestRedisBroker)

    def build_app(self, router):
        return router


class TestRouterPublisher(PublisherTestcase):
    broker_class = staticmethod(lambda: RedisRouter().broker)

    def build_app(self, router):
        return router
