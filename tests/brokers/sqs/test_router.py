import pytest

from faststream.sqs import SQSPublisher, SQSRoute
from tests.brokers.base.router import RouterLocalTestcase, RouterTestcase

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestRouter(SQSTestcaseConfig, RouterTestcase):
    route_class = SQSRoute
    publisher_class = SQSPublisher


@pytest.mark.connected()
@pytest.mark.sqs()
class TestRouterLocal(SQSMemoryTestcaseConfig, RouterLocalTestcase):
    route_class = SQSRoute
    publisher_class = SQSPublisher
