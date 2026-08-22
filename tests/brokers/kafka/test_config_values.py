import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Config
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import KafkaMemoryTestcaseConfig


@pytest.mark.kafka()
class TestConfigValues(KafkaMemoryTestcaseConfig, ConfigOverrideTestcase):
    @pytest.mark.asyncio()
    async def test_pattern_value(self, queue: str, event: asyncio.Event) -> None:
        broker = self.get_broker(config={"PATTERN": f"{queue}-.*"})

        @broker.subscriber(pattern=Config("PATTERN"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", f"{queue}-1")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_group_id_value(self, queue: str, mock: MagicMock) -> None:
        """Two subscribers land in one consumer group, so only one of them eats."""
        broker = self.get_broker(config={"GROUP": f"{queue}-group"})

        @broker.subscriber(queue, group_id=Config("GROUP"))
        async def resolved(msg: Any) -> None:
            mock("resolved")

        @broker.subscriber(queue, group_id=f"{queue}-group")
        async def literal(msg: Any) -> None:
            mock("literal")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        # Which of the two eats depends on iteration order; that only one does
        # is the assertion — an unresolved placeholder is a group of its own.
        assert mock.call_count == 1, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_none_default_leaves_the_subscriber_without_a_group(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """`None` is a real default: no group, so neither subscriber is deduped."""
        broker = self.get_broker()

        @broker.subscriber(queue, group_id=Config("ABSENT", default=None))
        async def first(msg: Any) -> None:
            mock("first")

        @broker.subscriber(queue, group_id=Config("ABSENT", default=None))
        async def second(msg: Any) -> None:
            mock("second")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        assert mock.call_count == 2, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_publisher_reply_to_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """The reply destination is configurable along with the primary one."""
        broker = self.get_broker(config={"REPLY": f"{queue}-reply"})

        publisher = broker.publisher(queue, reply_to=Config("REPLY"))

        @broker.subscriber(queue)
        async def handler(msg: Any) -> str:
            return "pong"

        @broker.subscriber(f"{queue}-reply")
        async def reply_handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("ping")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_log_line_names_the_resolved_topic(self, queue: str) -> None:
        broker = self.get_broker(config={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

            logger = br.config.logger.logger.logger
            topics = {
                call.kwargs["extra"]["topic"]
                for call in logger.log.call_args_list
                if call.kwargs.get("extra")
            }

        assert topics == {queue}, topics
