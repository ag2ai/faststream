from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.mq.fastapi import MQRouter
from faststream.mq.testing import TestMQBroker
from faststream.specification import Specification
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.mq()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = MQRouter
    router_class = MQRouter
    broker_wrapper = staticmethod(TestMQBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.mq()
class TestRouterPublisher(PublisherTestcase):
    broker_class = MQRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)
