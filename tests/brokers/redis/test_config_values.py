import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Config
from faststream.exceptions import SetupError
from faststream.params import Path
from faststream.redis import ListSub, PubSub, StreamSub
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import RedisMemoryTestcaseConfig


@pytest.mark.redis()
class TestConfigValues(RedisMemoryTestcaseConfig, ConfigOverrideTestcase):
    log_context_address_key = "channel"

    def get_config_value(self, address: str) -> Any:
        """Redis names a channel with a value object, so supply a real one."""
        return PubSub(address)

    @pytest.mark.asyncio()
    async def test_list_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(config_values={"IN": queue})

        @broker.subscriber(list=Config("IN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", list=queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_stream_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(config_values={"IN": queue})

        @broker.subscriber(stream=Config("IN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", stream=queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_prepared_list_object_as_a_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(
            config_values={"IN": ListSub(queue, polling_interval=0.01)}
        )

        @broker.subscriber(list=Config("IN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", list=queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_prepared_stream_object_as_a_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(
            config_values={"IN": StreamSub(queue, polling_interval=10)}
        )

        @broker.subscriber(stream=Config("IN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", stream=queue)

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_list_publisher_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"OUT": queue})

        publisher = broker.publisher(list=Config("OUT"))

        @broker.subscriber(list=queue)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_stream_publisher_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        broker = self.get_broker(config_values={"OUT": queue})

        publisher = broker.publisher(stream=Config("OUT"))

        @broker.subscriber(stream=queue)
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_a_config_value_holding_an_address_template_fills_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """A Config value is read as an Address template, exactly as a literal is."""
        broker = self.get_broker(
            apply_types=True,
            config_values={"IN": f"{queue}.{{level}}"},
        )

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None:
            mock(level)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}.info")

        mock.assert_called_once_with("info")

    @pytest.mark.asyncio()
    async def test_an_unsatisfiable_path_names_the_config_key(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True, config_values={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_config_value_that_is_not_a_template_names_the_config_key(
        self,
    ) -> None:
        broker = self.get_broker(config_values={"IN": "logs.${ENV"})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_batch_object_as_a_value_is_refused(self, queue: str) -> None:
        """`batch` picks the Subscriber class, so it cannot arrive from a value."""
        broker = self.get_broker(config_values={"IN": ListSub(queue, batch=True)})

        @broker.subscriber(list=Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker) as br:
                await br.start()

    @pytest.mark.asyncio()
    async def test_a_grouped_stream_as_a_value_is_refused(self, queue: str) -> None:
        """A consumer group picks the acknowledgement policy, read at build time."""
        broker = self.get_broker(
            config_values={"IN": StreamSub(queue, group="group", consumer="consumer")},
        )

        @broker.subscriber(stream=Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker) as br:
                await br.start()

    @pytest.mark.parametrize("address", ("list", "stream"))
    @pytest.mark.asyncio()
    async def test_an_unfillable_path_is_refused_on_every_address_kind(
        self,
        queue: str,
        address: str,
    ) -> None:
        """A list and a stream are read verbatim, so neither can fill a `Path()`.

        Redis matches a pattern on a channel only. Before this was checked, a
        `Path()` naming a list or a stream started cleanly and then handed the
        handler nothing for every message.
        """
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(**{address: f"{queue}.{{level}}"})
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="level"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.parametrize("address", ("list", "stream"))
    @pytest.mark.asyncio()
    async def test_an_unfillable_path_from_a_value_names_the_config_key(
        self,
        queue: str,
        address: str,
    ) -> None:
        broker = self.get_broker(
            apply_types=True, config_values={"IN": f"{queue}.{{level}}"}
        )

        @broker.subscriber(**{address: Config("IN")})
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match=r"Config value 'IN'"):
            async with self.patch_broker(broker):
                pass

    def test_a_prepared_channel_supplied_as_a_value_is_not_stamped(self) -> None:
        """The caller's object is theirs: `from_config_value` copies before stamping.

        The same prepared `PubSub` may stand as a Config value in one place and
        be used literally in another; stamping it in place would make the
        literal's failure name a Config key it never came from.
        """
        prepared = PubSub("channel")

        built = PubSub.from_config_value(prepared, "IN")

        assert prepared.address.config_key is None
        assert built.address.config_key == "IN"
