from typing import Any

import pytest
from typing_extensions import override

from faststream._internal.utils.path import Address
from faststream.exceptions import IncorrectState
from faststream.rabbit import ExchangeType, RabbitExchange, RabbitQueue
from tests.brokers.base.address import AddressCheckTestcase

from .basic import RabbitMemoryTestcaseConfig


@pytest.mark.rabbit()
class TestRabbitAddressTemplate(RabbitMemoryTestcaseConfig, AddressCheckTestcase):
    broker_address = "logs.*"

    @override
    def declare_subscriber(self, obj: Any, template: str) -> Any:
        return obj.subscriber(RabbitQueue("test", routing_key=template))

    @override
    def get_subscriber_address(self, subscriber: Any) -> Address:
        return subscriber.queue.routing_address


@pytest.mark.rabbit()
class TestEveryReadSettlesAtPreparation(RabbitMemoryTestcaseConfig):
    """RabbitMQ addresses an endpoint with more than one value.

    A queue, the routing key binding it, an exchange and a reply destination
    each arrive from their own option and each settles on its own terms
    (ADR-0003, ADR-0006). The queue's routing key is covered by the testcase
    above, which reads through it; these are the rest.
    """

    def test_reading_a_subscriber_queue_before_preparation_refuses(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, f"{queue}-exchange")

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.queue

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.exchange

        with pytest.raises(IncorrectState, match="too early"):
            _ = subscriber.declared_queue

    def test_reading_a_publisher_destination_before_preparation_refuses(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(
            queue,
            exchange=f"{queue}-exchange",
            routing_key=f"{queue}.info",
            reply_to="back",
        )

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.queue

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.exchange

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.routing_key

        with pytest.raises(IncorrectState, match="too early"):
            _ = publisher.reply_to

    def test_reading_a_declared_value_never_refuses(self, queue: str) -> None:
        """Only what depends on a Config value or the Router prefix is deferred."""
        broker = self.get_broker()

        subscriber = broker.subscriber(queue, f"{queue}-exchange")

        assert subscriber.calls is not None
        assert subscriber.ack_policy is not None
        assert repr(subscriber)

    @pytest.mark.asyncio()
    async def test_after_connect_every_read_answers(self, queue: str) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix="prefix_")

        exchange = f"{queue}-exchange"
        subscriber = router.subscriber(
            RabbitQueue(queue, routing_key=f"{queue}.info"),
            exchange,
        )
        publisher = router.publisher(
            queue,
            exchange=exchange,
            routing_key=f"{queue}.info",
            reply_to="back",
        )
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            # The queue and the routing key binding it wear the Router prefix.
            assert subscriber.queue.name == f"prefix_{queue}"
            assert subscriber.queue.routing() == f"prefix_{queue}.info"
            assert publisher.queue.name == f"prefix_{queue}"
            assert publisher.routing_key == f"prefix_{queue}.info"

            # The exchange lives outside the Router's namespace, and a reply
            # destination has never been decorated with the prefix either.
            assert subscriber.exchange.name == exchange
            assert publisher.exchange.name == exchange
            assert publisher.reply_to == "back"

            # What the log lines name is the declaration, undecorated.
            assert subscriber.declared_queue.name == queue

    @pytest.mark.asyncio()
    async def test_an_exchange_bound_to_another_resolves_down_the_chain(
        self,
        queue: str,
    ) -> None:
        """Preparation keeps the object, so the binding it carries survives it."""
        broker = self.get_broker()

        parent = RabbitExchange(f"{queue}-parent", type=ExchangeType.FANOUT)
        subscriber = broker.subscriber(
            queue,
            RabbitExchange(f"{queue}-nested", type=ExchangeType.FANOUT, bind_to=parent),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            assert subscriber.exchange.name == f"{queue}-nested"
            assert subscriber.exchange.bind_to is not None
            assert subscriber.exchange.bind_to.name == f"{queue}-parent"


@pytest.mark.rabbit()
def test_both_reads_fall_back_to_the_queue_name() -> None:
    queue = RabbitQueue("test")

    assert queue.routing_template() == "test"
    assert queue.routing() == "test"


@pytest.mark.rabbit()
def test_a_declared_routing_key_keeps_both_reads() -> None:
    queue = RabbitQueue("test", routing_key="logs.{level}")

    assert queue.routing_template() == "logs.{level}"
    assert queue.routing() == "logs.*"
