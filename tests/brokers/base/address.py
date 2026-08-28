import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import MagicMock

import pytest

from faststream import Path
from faststream.exceptions import SubscriberNotFound

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class AddressDeliveryTestcase(BaseTestcaseConfig):
    """A declaration reaches the wire exactly as written.

    Every test publishes to a concrete address and asserts what the handler
    received, never how a declaration was compiled. Addresses are built from the
    `queue` fixture, because the suite runs parallel against shared brokers.

    The four members below are the hooks a broker may respell; their examples
    show what each produces for `queue = "orders"` under the default `separator`.
    """

    separator = "."
    """What this broker writes between two address segments.

    Every address in every test is derived from it, so a broker that spells
    addresses another way usually declares nothing else: MQTT sets
    `separator = "/"` and gets `orders/{level}` out of the same code.
    """

    def declare_subscriber(self, obj: Any, declaration: str, queue: str) -> Any:
        """Subscribe `obj` to a declaration, however this broker spells that.

        `broker.subscriber("orders.{level}")` by default. Kafka takes the
        declaration behind `pattern=`, and RabbitMQ takes it as the routing key
        of a `RabbitQueue` named separately — hence `queue` alongside it.
        """
        args, kwargs = self.get_subscriber_params(declaration)
        return obj.subscriber(*args, **kwargs)

    def declare_publisher(self, obj: Any, declaration: str, queue: str) -> Any:
        """Give `obj` a publisher aimed at a declaration.

        `broker.publisher("orders.{{shard}}")` by default; RabbitMQ spells the
        same thing `broker.publisher(routing_key=..., exchange=...)`.
        """
        return obj.publisher(declaration)

    async def publish(self, broker: Any, address: str, message: str) -> None:
        """Send `message` to a concrete address, bypassing every declaration.

        `broker.publish("literal", "orders.{shard}")` by default — the address
        goes out as written, so nothing on this path reads a template.
        """
        await broker.publish(message, address)

    async def test_a_template_receives_only_its_own_addresses(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """A template names a family, and the parameter is read off the address."""
        sep = self.separator
        broker = self.get_broker(apply_types=True)

        subscriber = self.declare_subscriber(broker, f"{queue}{sep}{{level}}", queue)

        @subscriber
        async def handler(body: str, level: str = Path()) -> None:
            mock(body, level)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            # `other.orders.info` differs in its first segment, where every
            # broker anchors a pattern, so nothing declared here matches it.
            with suppress(SubscriberNotFound):
                # A real broker drops an unrouted message; an in-memory one says so.
                await self.publish(br, f"other{sep}{queue}{sep}info", "foreign")

            # Sent second, so the foreign message has had its chance to arrive.
            await self.publish(br, f"{queue}{sep}info", "matching")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("matching", "info")

    async def test_escaped_braces_subscribe_to_a_literal_address(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """`{{shard}}` is a brace the endpoint listens for, not a parameter."""
        sep = self.separator
        broker = self.get_broker()

        # `orders.{{shard}}` declares the address `orders.{shard}`.
        subscriber = self.declare_subscriber(broker, f"{queue}{sep}{{{{shard}}}}", queue)

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await self.publish(br, f"{queue}{sep}{{shard}}", "literal")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("literal")

    async def test_a_literal_brace_beside_a_parameter_is_not_regex_syntax(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """`{2}` put back into a capture regex quantifies what precedes it.

        The endpoint subscribes through its Broker address and receives the
        message either way; it is the parameter that goes missing, because the
        pattern that reads it off the address stops matching the address.
        """
        sep = self.separator
        broker = self.get_broker(apply_types=True)

        # `orders.{{2}}.{level}` declares `orders.{2}.info` and friends.
        declaration = f"{queue}{sep}{{{{2}}}}{sep}{{level}}"
        subscriber = self.declare_subscriber(broker, declaration, queue)

        @subscriber
        async def handler(body: str, level: str = Path()) -> None:
            mock(body, level)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await self.publish(br, f"{queue}{sep}{{2}}{sep}info", "quantified")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("quantified", "info")

    async def test_a_router_prefix_reaches_the_wire(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """A prefix decorates the declaration, escape included, before it compiles."""
        sep = self.separator
        broker = self.get_broker()
        router = self.get_router(prefix=f"prefix{sep}")

        subscriber = self.declare_subscriber(router, f"{queue}{sep}{{{{shard}}}}", queue)

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()
            await self.publish(br, f"prefix{sep}{queue}{sep}{{shard}}", "prefixed")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("prefixed")


class AddressPublisherDeliveryTestcase(AddressDeliveryTestcase):
    """Both ends of one declaration meet.

    Separate from the testcase above because a Publisher does not take a template
    on every broker: Kafka compiles an address only behind `pattern=`, which is a
    Subscriber-only argument.
    """

    async def test_a_publisher_reaches_a_subscriber_declared_the_same_way(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """Two endpoints written with one string used to miss each other.

        The subscriber listened on the restored address and the publisher sent to
        the declaration verbatim, escape and all.
        """
        broker = self.get_broker()

        declaration = f"{queue}{self.separator}{{{{shard}}}}"
        subscriber = self.declare_subscriber(broker, declaration, queue)

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        publisher = self.declare_publisher(broker, declaration, queue)

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("round-trip")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("round-trip")
