import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest
from typing_extensions import override

from faststream import Config
from faststream.confluent import KafkaBroker
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
class TestConfigValues(ConfluentMemoryTestcaseConfig, ConfigOverrideTestcase):
    @override
    def get_subscriber_params(
        self,
        *topics: Any,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Declare against `topics`, not `partitions`.

        Confluent's shared hook turns a single address into a `TopicPartition`
        so that a real broker replays from the start. A partition is not an
        address parameter — the option takes structures rather than names and
        stays out of the allowlist — so the placeholder would have nowhere to go.
        """
        return topics, {"auto_offset_reset": "earliest", **kwargs}

    @pytest.mark.asyncio()
    async def test_group_id_value(self, queue: str, mock: MagicMock) -> None:
        """Two subscribers land in one consumer group, so only one of them eats."""
        broker = self.get_broker(config_values={"GROUP": f"{queue}-group"})

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
        broker = self.get_broker(config_values={"REPLY": f"{queue}-reply"})

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
        broker = self.get_broker(config_values={"IN": queue})

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

    def test_the_startup_log_context_composes_the_prefix_once(
        self,
        queue: str,
    ) -> None:
        """The line an operator reads at startup names an address that exists.

        `topics` is a field Preparation writes — the Router prefix is already
        composed on it — so the log context must not compose the prefix a second
        time. Prepared first, because that is when the logger is built and so
        the earliest moment this line can be written.
        """
        router = self.get_router(prefix="prefix-")

        @router.subscriber(queue)
        async def handler(msg: Any) -> None: ...

        broker = self.get_broker()
        broker.include_router(router)
        broker.prepare()

        context = broker.subscribers[0].get_log_context(None)

        assert context["topic"] == f"prefix-{queue}", context


@pytest.mark.confluent()
def test_client_settings_still_reach_the_client() -> None:
    broker = KafkaBroker(config={"message.max.bytes": 1000})

    assert broker.config.connection_config.consumer_config["message.max.bytes"] == 1000
