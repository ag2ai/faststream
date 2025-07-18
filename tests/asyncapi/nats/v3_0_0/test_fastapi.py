import pytest

from typing import Any

from faststream._internal.broker import BrokerUsecase
from faststream.nats import TestNatsBroker
from faststream.nats.fastapi import NatsRouter
from faststream.specification.base import Specification
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.nats()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = NatsRouter
    router_class = NatsRouter
    broker_wrapper = staticmethod(TestNatsBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.nats()
class TestRouterPublisher(PublisherTestcase):
    broker_class = NatsRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)
