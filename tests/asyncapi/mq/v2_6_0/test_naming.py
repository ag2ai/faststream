import pytest

from faststream.mq import MQBroker
from tests.asyncapi.base.v2_6_0.naming import NamingTestCase


@pytest.mark.mq()
class TestNaming(NamingTestCase):
    broker_class = MQBroker
