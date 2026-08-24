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

    #: The key a Subscriber's log context files its address under. Each broker
    #: spells its own address kind — `topic`, `subject`, `channel`, `queue` —
    #: and that spelling is the only thing that varies in the log-line test.
    log_context_address_key: str

    #: Whether this broker's Publisher takes a reply destination at all.
    supports_reply_to = True

    def get_config_value(self, address: str) -> Any:
        """The Config value standing for `address`.

        Overridden by brokers whose addresses are value objects rather than
        plain names, so that "a prepared broker object works as a Config value"
        is asserted against a real prepared object.
        """
        return address

    def get_publisher_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """The arguments declaring a Publisher against `args[0]` as its address.

        The mirror of `get_subscriber_params`, overridden by brokers whose
        publisher takes its destination somewhere other than first positional.
        """
        return args, kwargs

    async def assert_consume(
        self,
        broker: Any,
        address: str,
        event: asyncio.Event,
        body: Any = "hello",
    ) -> None:
        await broker.start()
        await broker.publish(body, address)

        with anyio.move_on_after(self.timeout):
            await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_broker_level_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(config_values={"IN": queue})

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

        FastStream(broker, config_values={"IN": queue})

        async with self.patch_broker(broker) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_broker_level_wins_over_app_level(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"IN": queue})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        FastStream(broker, config_values={"IN": f"{queue}-app"})

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

        broker = self.get_broker(config_values=Settings())

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
        broker = self.get_broker(config_values={"IN": self.get_config_value(queue)})

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

        broker = self.get_broker(config_values={"IN": queue})
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

        broker = self.get_broker(config_values={"IN": queue})
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

        FastStream(broker, config_values={"IN": queue})

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

        broker = self.get_broker(config_values={"IN": queue})
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
    async def test_publisher_sends_to_the_resolved_address(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"OUT": queue})

        args, kwargs = self.get_publisher_params(Config("OUT"))
        publisher = broker.publisher(*args, **kwargs)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_publisher_resolved_value_ignores_the_router_prefix(
        self,
        queue: str,
        mock: MagicMock,
        event: asyncio.Event,
        event2: asyncio.Event,
    ) -> None:
        """A Publisher's resolved destination is used as supplied; a literal is prefixed."""
        router = self.get_router(prefix="prefix-")

        args, kwargs = self.get_publisher_params(Config("OUT"))
        resolved_publisher = router.publisher(*args, **kwargs)

        args, kwargs = self.get_publisher_params(f"literal-{queue}")
        literal_publisher = router.publisher(*args, **kwargs)

        broker = self.get_broker(config_values={"OUT": queue})

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def resolved(msg: Any) -> None:
            mock("resolved")
            event.set()

        args, kwargs = self.get_subscriber_params(f"prefix-literal-{queue}")

        @broker.subscriber(*args, **kwargs)
        async def literal(msg: Any) -> None:
            mock("literal")
            event2.set()

        broker.include_router(router)

        async with self.patch_broker(broker) as br:
            await br.start()

            await resolved_publisher.publish("hello")
            await literal_publisher.publish("hello")

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
        broker = self.get_broker(config_values={"IN": queue})

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

    @pytest.mark.asyncio()
    async def test_log_line_names_the_resolved_address(self, queue: str) -> None:
        """An operator reads the address messages arrive on, not the placeholder.

        The log context is built from what Preparation resolved, so a Subscriber
        declared against `Config("IN")` files its lines under the address the
        value named. A context built from the declaration would name `IN`.
        """
        broker = self.get_broker(config_values={"IN": queue})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

            logger = br.config.logger.logger.logger
            addresses = {
                call.kwargs["extra"][self.log_context_address_key]
                for call in logger.log.call_args_list
                if call.kwargs.get("extra")
            }

        assert addresses == {queue}, addresses

    @pytest.mark.asyncio()
    async def test_publisher_reply_to_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """The reply destination is configurable along with the primary one."""
        if not self.supports_reply_to:
            pytest.skip("broker publisher takes no reply destination")

        broker = self.get_broker(config_values={"REPLY": f"{queue}-reply"})

        pub_args, pub_kwargs = self.get_publisher_params(
            queue,
            reply_to=Config("REPLY"),
        )
        publisher = broker.publisher(*pub_args, **pub_kwargs)

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> str:
            return "pong"

        reply_args, reply_kwargs = self.get_subscriber_params(f"{queue}-reply")

        @broker.subscriber(*reply_args, **reply_kwargs)
        async def reply_handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("ping")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()


class ConfigOverrideTestcase(ConfigTestcase):
    """The test broker's own Config values, which beat the Broker's."""

    @pytest.mark.asyncio()
    async def test_test_broker_override_wins(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"IN": f"{queue}-real"})

        args, kwargs = self.get_subscriber_params(Config("IN"))

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker, config_values={"IN": queue}) as br:
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

        async with self.patch_broker(broker, config_values={"IN": queue}) as br:
            await self.assert_consume(br, queue, event)

    @pytest.mark.asyncio()
    async def test_a_second_context_uses_its_own_values(self, queue: str) -> None:
        """Each context prepares the Broker anew, against the values it was given.

        ADR-0004 fixes a Config value at `connect()`. Without invalidation the
        code fixes it at the *first* `connect()`, and the second context here
        would listen on — and publish to — the first one's address.
        """
        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(Config("IN"))

        events = {
            f"first-{queue}": asyncio.Event(),
            f"second-{queue}": asyncio.Event(),
        }

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: Any) -> None:
            events[msg].set()

        for address, event in events.items():
            async with self.patch_broker(
                broker,
                config_values={"IN": address},
            ) as br:
                # The body names the address, so each context's handler is seen
                # to have received what that context published.
                await self.assert_consume(br, address, event, body=address)
