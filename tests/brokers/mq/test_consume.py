import pytest

from tests.brokers.base.consume import BrokerConsumeTestcase

from .basic import MQMemoryTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestConsume(MQMemoryTestcaseConfig, BrokerConsumeTestcase):
    async def test_subscriber_sets_header_max_value_length(self, queue: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, header_max_value_length=123)

        assert subscriber.header_max_value_length == 123
