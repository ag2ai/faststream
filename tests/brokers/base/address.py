import asyncio
from typing import Annotated, Any
from unittest.mock import MagicMock

import pytest

from faststream import Path

from .basic import BaseTestcaseConfig


@pytest.mark.asyncio()
class AddressDeliveryTestcase(BaseTestcaseConfig):
    separator = "."
    """What goes between two address segments; MQTT spells it `/`."""

    def declare_subscriber(self, obj: Any, declaration: str, queue: str) -> Any:
        """Kafka respells this with `pattern=`, RabbitMQ with a `RabbitQueue` key."""
        args, kwargs = self.get_subscriber_params(declaration)
        return obj.subscriber(*args, **kwargs)

    def declare_publisher(self, obj: Any, declaration: str, queue: str) -> Any:
        """Aim a publisher at a declaration; RabbitMQ respells it as a routing key."""
        return obj.publisher(declaration)

    async def publish(self, broker: Any, address: str, message: str) -> None:
        """Send to a concrete address; RabbitMQ respells it as a routing key."""
        await broker.publish(message, address)

    async def test_a_literal_brace_beside_a_parameter_is_not_regex_syntax(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(apply_types=True)

        # subscribe to "queue.{2}.{level}", where `{2}` is literal
        subscriber = self.declare_subscriber(
            broker,
            f"{queue}{self.separator}{{{{2}}}}{self.separator}{{level}}",
            queue,
        )

        @subscriber
        async def handler(
            body: str,
            level: Annotated[str, Path()],
        ) -> None:
            mock(body, level)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            # publish to "queue.{2}.info"
            await self.publish(
                br,
                f"{queue}{self.separator}{{2}}{self.separator}info",
                "quantified",
            )
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("quantified", "info")

    async def test_a_router_prefix_reaches_the_wire(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker()
        router = self.get_router(prefix=f"prefix{self.separator}")

        # subscribe to literal "prefix.queue.{shard}"
        subscriber = self.declare_subscriber(
            router,
            f"{queue}{self.separator}{{{{shard}}}}",
            queue,
        )

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            # publish to "prefix.queue.{shard}"
            await self.publish(
                br,
                f"prefix{self.separator}{queue}{self.separator}{{shard}}",
                "prefixed",
            )
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("prefixed")


class AddressPublisherDeliveryTestcase(AddressDeliveryTestcase):
    async def test_a_publisher_reaches_a_subscriber_declared_the_same_way(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker()

        # both ends read one declaration of "queue.{shard}"
        declaration = f"{queue}{self.separator}{{{{shard}}}}"
        subscriber = self.declare_subscriber(broker, declaration, queue)

        @subscriber
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        # the publisher used to send to the declaration verbatim, escape and all,
        # while the subscriber listened on the restored address
        publisher = self.declare_publisher(broker, declaration, queue)

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("round-trip")
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        mock.assert_called_once_with("round-trip")
