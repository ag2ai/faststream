from faststream.nats import TestNatsBroker
from faststream.nats.fastapi import NatsRouter
from tests.asyncapi.base.v2_6_0.arguments import FastAPICompatible
from tests.asyncapi.base.v2_6_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v2_6_0.publisher import PublisherTestcase


class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = staticmethod(lambda: NatsRouter().broker)
    router_class = NatsRouter
    broker_wrapper = staticmethod(TestNatsBroker)

    def build_app(self, router):
        return router


class TestRouterPublisher(PublisherTestcase):
    broker_class = staticmethod(lambda: NatsRouter().broker)

    def build_app(self, router):
        return router
