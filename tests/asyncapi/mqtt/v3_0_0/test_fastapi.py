from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.mqtt.fastapi import MQTTRouter
from faststream.mqtt.testing import TestMQTTBroker
from faststream.specification import Specification
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.mqtt()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = MQTTRouter
    router_class = MQTTRouter
    broker_wrapper = staticmethod(TestMQTTBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.mqtt()
class TestRouterPublisher(PublisherTestcase):
    broker_class = MQTTRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)
