import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Config, FastStream
from faststream.exceptions import SetupError

from .basic import BaseTestcaseConfig


class ConfigTestcase(BaseTestcaseConfig):
    """Config values, observed the only way a user can observe them.

    Every assertion here is a round trip: a Subscriber declared with a Config
    placeholder receives what is published to the address the placeholder
    resolves to. Nothing reaches into the endpoint's options.
    """

    def get_config_value(self, address: str) -> Any:
        """The Config value standing for `address`.

        Overridden by brokers whose addresses are value objects rather than
        plain names, so that "a prepared broker object works as a Config value"
        is asserted against a real prepared object.
        """
        return address

    async def assert_consume(
        self,
        broker: Any,
        address: str,
        event: asyncio.Event,
    ) -> None:
        await broker.start()
        await broker.publish("hello", address)

        with anyio.move_on_after(self.timeout):
            await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_broker_level_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(config={"IN": queue})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_app_level_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        FastStream(broker, config={"IN": queue})

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_broker_level_wins_over_app_level(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config={"IN": queue})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        FastStream(broker, config={"IN": f"{queue}-app"})

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_value_supplied_by_a_settings_object(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        class Settings:
            IN = queue

        broker = self.get_broker(config=Settings())

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_prepared_object_as_a_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config={"IN": self.get_config_value(queue)})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_default_is_used(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(Config("ABSENT", default=queue))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_router_inclusion(self, queue: str, event: asyncio.Event) -> None:
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @router.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        broker = self.get_broker(config={"IN": queue})
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_nested_router_inclusion(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        inner = self.get_router()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @inner.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        outer = self.get_router()
        outer.include_router(inner)

        broker = self.get_broker(config={"IN": queue})
        broker.include_router(outer)

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_broker_added_to_the_app_after_router_inclusion(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        router = self.get_router()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @router.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        broker = self.get_broker()
        broker.include_router(router)

        FastStream(broker, config={"IN": queue})

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_resolved_value_ignores_the_router_prefix(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        """A resolved address is used as supplied; a literal beside it is prefixed."""
        router = self.get_router(prefix="prefix-")

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @router.subscriber(*args, **kwargs)
        async def resolved(msg: Any) -> None:
            mock("resolved")
            event.set()

        args, kwargs = self.get_subscriber_params(f"literal-{queue}")

        @router.subscriber(*args, **kwargs)
        async def literal(msg: Any) -> None:
            mock("literal")
            event2.set()

        broker = self.get_broker(config={"IN": queue})
        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue)
            await br.publish("hello", f"prefix-literal-{queue}")

            with anyio.move_on_after(self.timeout):
                await event.wait()
                await event2.wait()

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_subscriber_created_after_the_broker_started(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config={"IN": queue})

        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            args, kwargs = self.get_subscriber_params(Config("IN"))
            sub = br.subscriber(*args, **kwargs)
            sub(handler)
            await sub.start()

            await br.publish("hello", queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

            await sub.stop()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_missing_value_raises_at_startup(self, queue: str) -> None:
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(Config("ABSENT"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match="ABSENT"):
            async with self.patch_broker(broker) as br:
                await br.start()


class ConfigOverrideTestcase(ConfigTestcase):
    """The test broker's own Config values, which beat the Broker's."""

    @pytest.mark.asyncio()
    async def test_test_broker_override_wins(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config={"IN": f"{queue}-real"})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker, config={"IN": queue}) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_test_broker_supplies_the_app_level_values(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Broker whose values live on its App is testable on its own."""
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker, config={"IN": queue}) as br:
            await self.assert_consume(br, queue, event)
