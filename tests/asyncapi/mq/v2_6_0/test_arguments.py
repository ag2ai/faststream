import pytest

from faststream.mq import MQBroker
from tests.asyncapi.base.v2_6_0.arguments import ArgumentsTestcase


@pytest.mark.mq()
class TestArguments(ArgumentsTestcase):
    broker_class = MQBroker
