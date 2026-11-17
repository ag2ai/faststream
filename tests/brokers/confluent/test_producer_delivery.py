import asyncio
from collections.abc import AsyncIterator
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import pytest_asyncio
from anyio.lowlevel import checkpoint
from confluent_kafka import KafkaError, KafkaException, Message

from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers.client import AsyncConfluentProducer
from faststream.confluent.helpers.config import ConfluentFastConfig


@pytest_asyncio.fixture()
async def delivery() -> AsyncIterator[
    tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]]
]:
    loop = asyncio.get_running_loop()
    previous_handler = loop.get_exception_handler()
    errors: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: errors.append(context))

    native = MagicMock()
    with patch("faststream.confluent.helpers.client.Producer", return_value=native):
        producer = AsyncConfluentProducer(
            logger=LoggerState(),
            config=ConfluentFastConfig(),
        )
        try:
            yield producer, native, errors
        finally:
            await producer.stop()
            loop.set_exception_handler(previous_handler)


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestProducerDelivery:
    async def test_cancelled_publish_ignores_late_success(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        message = MagicMock(spec=Message)
        message.error.return_value = None
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        task.cancel()
        await checkpoint()
        acknowledge(None, message)
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_cancelled_publish_ignores_late_callback_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        error = KafkaError(KafkaError._MSG_TIMED_OUT)
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        task.cancel()
        await checkpoint()
        acknowledge(error, None)
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_cancelled_publish_ignores_late_message_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        message = MagicMock(spec=Message)
        message.error.return_value = KafkaError(KafkaError._MSG_TIMED_OUT)
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        task.cancel()
        await checkpoint()
        acknowledge(None, message)
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_cancelled_publish_ignores_queued_success(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        message = MagicMock(spec=Message)
        message.error.return_value = None
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(None, message)
        task.cancel()
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_cancelled_publish_ignores_queued_callback_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        error = KafkaError(KafkaError._MSG_TIMED_OUT)
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(error, None)
        task.cancel()
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_cancelled_publish_ignores_queued_message_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        message = MagicMock(spec=Message)
        message.error.return_value = KafkaError(KafkaError._MSG_TIMED_OUT)
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(None, message)
        task.cancel()
        await checkpoint()

        # Assert
        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    async def test_queued_error_preserves_success(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        error = KafkaError(KafkaError._MSG_TIMED_OUT)
        message = MagicMock(spec=Message)
        message.error.return_value = None
        future = cast(
            "asyncio.Future[Message | None]",
            await producer.send("topic", value=b"payload", no_confirm=True),
        )
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(None, message)
        acknowledge(error, None)
        await checkpoint()

        # Assert
        assert future.result() is message
        assert errors == []

    async def test_queued_success_preserves_callback_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        error = KafkaError(KafkaError._MSG_TIMED_OUT)
        future = cast(
            "asyncio.Future[Message | None]",
            await producer.send("topic", value=b"payload", no_confirm=True),
        )
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(error, None)
        acknowledge(None, None)
        await checkpoint()

        # Assert
        with pytest.raises(KafkaException) as exc_info:
            future.result()
        assert exc_info.value.args == (error,)
        assert errors == []

    async def test_queued_success_preserves_message_error(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        # Arrange
        producer, native, errors = delivery
        error = KafkaError(KafkaError._MSG_TIMED_OUT)
        message = MagicMock(spec=Message)
        message.error.return_value = error
        future = cast(
            "asyncio.Future[Message | None]",
            await producer.send("topic", value=b"payload", no_confirm=True),
        )
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        # Act
        acknowledge(None, message)
        acknowledge(None, None)
        await checkpoint()

        # Assert
        with pytest.raises(KafkaException) as exc_info:
            future.result()
        assert exc_info.value.args == (error,)
        assert errors == []
