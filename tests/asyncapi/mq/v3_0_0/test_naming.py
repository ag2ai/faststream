import pytest

from faststream.mq import MQBroker
from tests.asyncapi.base.v3_0_0.naming import NamingTestCase


@pytest.mark.mq()
class TestNaming(NamingTestCase):
    broker_class = MQBroker
