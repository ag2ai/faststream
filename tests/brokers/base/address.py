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

    One declaration is read twice — an endpoint subscribes through the compiled
    Broker address, while a publisher and a Router prefix read the template — and
    nothing in the type system stops the two reads from disagreeing. These tests
    never look at either read. They publish to a concrete address and assert what
    the handler received, which is the only thing a user can observe and the only
    thing the two reads agreeing actually buys.

    `separator` is the one piece of address syntax brokers spell differently.
    Everything else is derived from it, so most subclasses declare nothing at all.

    Every address is built from the `queue` fixture, because the suite runs
    parallel against shared brokers and a fixed address would cross-talk.
    """

    separator = "."

    def template(self, queue: str) -> str:
        """A declaration with one Path parameter."""
        return f"{queue}{self.separator}{{level}}"

    def matching_address(self, queue: str) -> str:
        """A concrete address the template stands for."""
        return f"{queue}{self.separator}info"

    def foreign_address(self, queue: str) -> str:
        """A concrete address outside the template's family.

        Differs in its first segment: every broker anchors a pattern at the start
        of an address, so this one is the reliably-unmatched address everywhere.
        """
        return f"other{self.separator}{queue}{self.separator}info"

    def escaped_declaration(self, queue: str) -> str:
        """A declaration asking for a literal brace rather than a parameter."""
        return f"{queue}{self.separator}{{{{shard}}}}"

    def literal_address(self, queue: str) -> str:
        """The concrete address `escaped_declaration` asks for."""
        return f"{queue}{self.separator}{{shard}}"

    def declare_subscriber(self, obj: Any, declaration: str, queue: str) -> Any:
        """Subscribe `obj` to a declaration, however this broker spells that."""
        args, kwargs = self.get_subscriber_params(declaration)
        return obj.subscriber(*args, **kwargs)

    def declare_publisher(self, obj: Any, declaration: str, queue: str) -> Any:
        """Give `obj` a publisher aimed at a declaration."""
        return obj.publisher(declaration)

    async def publish(self, broker: Any, address: str, message: str) -> None:
        """Send `message` to a concrete address, bypassing every declaration."""
        await broker.publish(message, address)

    async def publish_unrouted(self, broker: Any, address: str, message: str) -> None:
        """Send to an address no endpoint here declared.

        A real broker drops it silently; an in-memory one has nowhere to route it
        and says so. Both mean the same thing, and neither is what is under test —
        what matters is that the handler below does not see it.
        """
        with suppress(SubscriberNotFound):
            await self.publish(broker, address, message)

    async def test_a_template_receives_only_its_own_addresses(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """A template names a family, and the parameter is read off the address."""
        broker = self.get_broker(apply_types=True)

        subscriber = self.declare_subscriber(broker, self.template(queue), queue)

        @subscriber
        async def handler(body: str, level: str = Path()) -> None:
            mock(body, level)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            # Published first, so it has had its chance by the time the second
            # message arrives and releases the assertion below.
            await self.publish_unrouted(br, self.foreign_address(queue), "foreign")
            await self.publish(br, self.matching_address(queue), "matching")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("matching", "info")

    async def test_escaped_braces_subscribe_to_a_literal_address(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """`{{shard}}` is a brace the endpoint listens for, not a parameter."""
        broker = self.get_broker()

        subscriber = self.declare_subscriber(
            broker,
            self.escaped_declaration(queue),
            queue,
        )

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await self.publish(br, self.literal_address(queue), "literal")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("literal")

    async def test_a_router_prefix_reaches_the_wire(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        """A prefix decorates the declaration, escape included, before it compiles."""
        broker = self.get_broker()
        router = self.get_router(prefix=f"prefix{self.separator}")

        subscriber = self.declare_subscriber(
            router,
            self.escaped_declaration(queue),
            queue,
        )

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()
            await self.publish(
                br,
                f"prefix{self.separator}{self.literal_address(queue)}",
                "prefixed",
            )
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

        declaration = self.escaped_declaration(queue)
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
