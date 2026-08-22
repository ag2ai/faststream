from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.redis.schemas import PubSub
from tests.brokers.base.address import AddressTemplateTestcase

from .basic import RedisTestcaseConfig


@pytest.mark.redis()
class TestRedisAddressTemplate(RedisTestcaseConfig, AddressTemplateTestcase):
    broker_address = "logs.*"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.channel.address

    def test_publisher_reads_through_the_same_address(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        publisher = router.publisher("out.{id}")
        broker.include_router(router)

        assert publisher.channel.address.template == "prefix_out.{id}"
        assert publisher.channel.name == "prefix_out.*"


@pytest.mark.redis()
def test_pattern_is_a_flag_not_the_template() -> None:
    assert PubSub("logs.{level}").pattern is True
    assert PubSub("logs.*").pattern is True
    assert PubSub("logs", pattern=True).pattern is True
    assert PubSub("logs").pattern is False
