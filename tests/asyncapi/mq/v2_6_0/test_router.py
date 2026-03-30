import pytest

from faststream.mq import MQBroker
from faststream.mq.broker.router import MQPublisher, MQRoute, MQRouter
from tests.asyncapi.base.v2_6_0.router import RouterTestcase


@pytest.mark.mq()
class TestRouter(RouterTestcase):
    broker_class = MQBroker
    router_class = MQRouter
    publisher_class = MQPublisher
    route_class = MQRoute
