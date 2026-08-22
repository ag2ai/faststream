import asyncio
from typing import Any
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Config
from faststream.exceptions import SetupError
from faststream.params import Path
from tests.brokers.base.config import ConfigOverrideTestcase

from .basic import NatsMemoryTestcaseConfig, NatsTestcaseConfig


@pytest.mark.nats()
class TestConfigValues(NatsMemoryTestcaseConfig, ConfigOverrideTestcase):
    @pytest.mark.asyncio()
    async def test_queue_group_value(self, queue: str, mock: MagicMock) -> None:
        """Two subscribers land in one queue group, so only one of them eats."""
        broker = self.get_broker(config={"GROUP": f"{queue}-group"})

        @broker.subscriber(queue, queue=Config("GROUP"))
        async def resolved(msg: Any) -> None:
            mock("resolved")

        @broker.subscriber(queue, queue=f"{queue}-group")
        async def literal(msg: Any) -> None:
            mock("literal")

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

        # Which of the two eats depends on iteration order; that only one does
        # is the assertion — an unresolved placeholder is a group of its own.
        assert mock.call_count == 1, mock.call_args_list

    @pytest.mark.asyncio()
    async def test_stream_value(self, queue: str, event: asyncio.Event) -> None:
        """A message published to the resolved stream reaches the subscriber."""
        broker = self.get_broker(config={"STREAM": f"{queue}-stream"})

        @broker.subscriber(queue, stream=Config("STREAM"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue, stream=f"{queue}-stream")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_a_subscriber_declared_entirely_from_config(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """Subject, queue group and stream all come from Config values at once."""
        broker = self.get_broker(
            config={
                "SUBJECT": queue,
                "GROUP": f"{queue}-group",
                "STREAM": f"{queue}-stream",
            },
        )

        @broker.subscriber(
            Config("SUBJECT"),
            queue=Config("GROUP"),
            stream=Config("STREAM"),
        )
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue, stream=f"{queue}-stream")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_durable_name_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A durable consumer named from a Config value still receives messages."""
        broker = self.get_broker(
            config={"STREAM": f"{queue}-stream", "DURABLE": f"{queue}-durable"},
        )

        @broker.subscriber(
            queue,
            stream=Config("STREAM"),
            durable=Config("DURABLE"),
            pull_sub=True,
        )
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue, stream=f"{queue}-stream")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_publisher_stream_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A Publisher's stream is configurable along with its subject."""
        broker = self.get_broker(
            config={"OUT": queue, "STREAM": f"{queue}-stream"},
        )

        publisher = broker.publisher(Config("OUT"), stream=Config("STREAM"))

        @broker.subscriber(queue, stream=f"{queue}-stream")
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish("hello")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()

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
    async def test_log_line_names_the_resolved_subject(self, queue: str) -> None:
        broker = self.get_broker(config={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", queue)

            logger = br.config.logger.logger.logger
            subjects = {
                call.kwargs["extra"]["subject"]
                for call in logger.log.call_args_list
                if call.kwargs.get("extra")
            }

        assert subjects == {queue}, subjects

    @pytest.mark.asyncio()
    async def test_a_config_value_holding_an_address_template_fills_path(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """A Config value is read as an Address template, exactly as a literal is."""
        broker = self.get_broker(
            apply_types=True,
            config={"IN": f"{queue}.{{level}}"},
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
        broker = self.get_broker(apply_types=True, config={"IN": queue})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any, level: str = Path()) -> None: ...

        with pytest.raises(SetupError, match="IN"):
            async with self.patch_broker(broker):
                pass

    @pytest.mark.asyncio()
    async def test_a_config_value_that_is_not_a_template_names_the_config_key(
        self,
    ) -> None:
        broker = self.get_broker(config={"IN": "logs.${ENV"})

        @broker.subscriber(Config("IN"))
        async def handler(msg: Any) -> None: ...

        with pytest.raises(SetupError, match="IN"):
            async with self.patch_broker(broker):
                pass


@pytest.mark.connected()
@pytest.mark.nats()
class TestConfigValuesConnected(NatsTestcaseConfig):
    """The addresses only a real NATS server can answer for."""

    timeout: float = 10.0

    @pytest.mark.asyncio()
    async def test_key_value_bucket_value(
        self,
        queue: str,
        event: asyncio.Event,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker(
            apply_types=True,
            config={"BUCKET": f"{queue}-bucket"},
        )

        @broker.subscriber(queue, kv_watch=Config("BUCKET"))
        async def handler(msg: Any) -> None:
            mock(msg)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.key_value(f"{queue}-bucket")

            await asyncio.wait(
                (
                    asyncio.create_task(bucket.put(queue, b"world")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_with(b"world")

    @pytest.mark.asyncio()
    async def test_object_storage_bucket_value(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """An object watch is named by its subject, so a placeholder there names it."""
        broker = self.get_broker(apply_types=True, config={"BUCKET": queue})

        @broker.subscriber(Config("BUCKET"), obj_watch=True)
        async def handler(filename: str) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            bucket = await br.object_storage(queue)

            await asyncio.wait(
                (
                    asyncio.create_task(bucket.put("hello", b"world")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()

    @pytest.mark.asyncio()
    async def test_stream_is_declared_under_the_resolved_name(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """The stream a placeholder resolves to is the one created and consumed from."""
        broker = self.get_broker(config={"STREAM": f"{queue}-stream"})

        @broker.subscriber(queue, stream=Config("STREAM"))
        async def handler(msg: Any) -> None:
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("hello", queue, stream=f"{queue}-stream")

            with anyio.move_on_after(self.timeout):
                await event.wait()

        assert event.is_set()
