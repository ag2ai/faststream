"""What a Confluent address can and cannot promise.

Confluent subscribes by topic only — it has no pattern subscription, and its
parser never matches an incoming message against a capture regex. Every address
it holds is therefore a literal one: what it says is what it subscribes to, and
no `Path()` parameter can ever be filled from it.

That is worth checking rather than assuming, because the failure it prevents is
silent. Without the `connect()`-time read, `subscriber("logs.{level}")` would be
accepted and then hand the handler no `level` for every message it received.
"""

import asyncio
from typing import Any

import anyio
import pytest

from faststream import Config
from faststream.confluent import TopicPartition
from faststream.exceptions import SetupError
from faststream.params import Path

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestConfluentAddress(ConfluentMemoryTestcaseConfig):
    @pytest.mark.asyncio()
    async def test_a_path_parameter_is_refused_at_connect(self) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber("logs.{level}")
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="level"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_path_parameter_with_a_default_is_accepted(self) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber("logs.{level}")
        async def handler(msg: Any, level: str = Path(default="unknown")) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()

    @pytest.mark.asyncio()
    async def test_a_partition_address_is_read_too(self, queue: str) -> None:
        """A partition names a topic, so it is an address a message arrives on."""
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(partitions=[TopicPartition(f"{queue}.{{level}}")])
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="level"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_config_value_holding_a_template_names_the_key(self) -> None:
        """A Config value is read exactly as a literal declaration is — literally."""
        broker = self.get_broker(apply_types=True, config_values={"IN": "logs.{level}"})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="IN"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_brace_in_a_topic_is_just_a_character(
        self,
        event: asyncio.Event,
    ) -> None:
        """Nothing compiles, so nothing can refuse a brace that spells no parameter."""
        broker = self.get_broker(config_values={"IN": "logs.${ENV"})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", "logs.${ENV")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()
