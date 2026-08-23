from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import IncorrectState
from faststream.redis.schemas import PubSub
from tests.brokers.base.address import AddressCheckTestcase

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
class TestRedisAddressTemplate(RedisMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs.*"

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.channel.address

    @override
    def test_an_early_read_does_not_pin_an_outer_router_prefix(self) -> None:
        """The read Redis refuses, in place of the one it used to defend against.

        A Router included into another Router composes a longer prefix than its
        endpoints saw when they were declared. Asked in between, a Redis
        Subscriber says the read came too early instead of answering with a
        channel built from a composition that is not final.
        """
        broker = self.get_broker()
        outer = self.get_router(prefix="outer_")
        inner = self.get_router(prefix="inner_")

        subscriber = self.declare_subscriber(inner, self.template)

        with pytest.raises(IncorrectState, match="too early"):
            self.get_subscriber_address(subscriber)

        outer.include_router(inner)
        broker.include_router(outer)

        address = self.read_address(broker.subscribers[0])
        assert address.template == f"outer_inner_{self.template}"
        assert address.broker_address == f"outer_inner_{self.broker_address}"

    def test_publisher_reads_through_the_same_address(self) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        publisher = router.publisher("out.{id}")
        broker.include_router(router)
        publisher.prepare()

        assert publisher.channel.address.template == "prefix_out.{id}"
        assert publisher.channel.name == "prefix_out.*"


@pytest.mark.redis()
def test_pattern_is_a_flag_not_the_template() -> None:
    assert PubSub("logs.{level}").pattern is True
    assert PubSub("logs.*").pattern is True
    assert PubSub("logs", pattern=True).pattern is True
    assert PubSub("logs").pattern is False


@pytest.mark.redis()
@pytest.mark.parametrize("kind", ("list", "stream"))
class TestEveryAddressKindSettlesAtPreparation(RedisMemoryTestcaseConfig):
    """A list and a stream answer on the same terms a channel does.

    Redis names each of its three address kinds with its own value object, built
    by its own constructor, so each is somewhere the rule could be missed. The
    channel is covered by the testcase above, which reads through one.
    """

    def test_reading_before_preparation_refuses(self, queue: str, kind: str) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(**{kind: queue})

        with pytest.raises(IncorrectState, match="too early"):
            getattr(subscriber, f"{kind}_sub")

    @pytest.mark.asyncio()
    async def test_after_connect_the_same_read_answers(
        self,
        queue: str,
        kind: str,
    ) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        subscriber = router.subscriber(**{kind: queue})
        publisher = router.publisher(**{kind: queue})
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            assert getattr(subscriber, f"{kind}_sub").name == f"prefix_{queue}"
            assert getattr(publisher, kind).name == f"prefix_{queue}"
