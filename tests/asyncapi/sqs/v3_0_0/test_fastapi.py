from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase
from faststream.specification import Specification
from faststream.sqs.fastapi import SQSRouter
from faststream.sqs.testing import TestSQSBroker
from tests.asyncapi.base.v3_0_0.arguments import FastAPICompatible
from tests.asyncapi.base.v3_0_0.fastapi import FastAPITestCase
from tests.asyncapi.base.v3_0_0.publisher import PublisherTestcase


@pytest.mark.sqs()
class TestRouterArguments(FastAPITestCase, FastAPICompatible):
    broker_class = SQSRouter
    router_class = SQSRouter
    broker_wrapper = staticmethod(TestSQSBroker)

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)


@pytest.mark.sqs()
class TestRouterPublisher(PublisherTestcase):
    broker_class = SQSRouter

    def get_spec(self, broker: BrokerUsecase[Any, Any]) -> Specification:
        return super().get_spec(broker.broker)
