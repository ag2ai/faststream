import asyncio
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import MagicMock, Mock, patch
from weakref import ref

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
    async def test_cancelled_publish_ignores_queued_delivery(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        producer, native, errors = delivery
        task = asyncio.create_task(producer.send("topic", value=b"payload"))
        await checkpoint()
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        acknowledge(None, None)
        task.cancel()
        await checkpoint()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert errors == []

    @pytest.mark.parametrize(
        "first_error",
        (
            pytest.param(None, id="success-before-error"),
            pytest.param(
                KafkaError(KafkaError._MSG_TIMED_OUT),
                id="error-before-success",
            ),
        ),
    )
    async def test_worker_deliveries_preserve_first_outcome(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
        first_error: KafkaError | None,
    ) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/3259."""
        producer, native, errors = delivery
        message = MagicMock(spec=Message)
        message.error.return_value = None
        second_error = (
            KafkaError(KafkaError._MSG_TIMED_OUT) if first_error is None else None
        )
        future = await producer.send("topic", value=b"payload", no_confirm=True)
        assert isinstance(future, asyncio.Future)
        acknowledge = native.produce.call_args.kwargs["on_delivery"]

        def deliver_twice() -> None:
            acknowledge(first_error, message)
            acknowledge(second_error, message)

        loop = asyncio.get_running_loop()
        previous_debug = loop.get_debug()
        try:
            loop.set_debug(True)
            await asyncio.to_thread(deliver_twice)
            await checkpoint()
        finally:
            loop.set_debug(previous_debug)

        if first_error is None:
            assert future.result() is message
        else:
            with pytest.raises(KafkaException) as exc_info:
                future.result()
            assert exc_info.value.args == (first_error,)
        assert errors == []

    async def test_failed_delivery_releases_message_before_completion(
        self,
        delivery: tuple[AsyncConfluentProducer, MagicMock, list[dict[str, Any]]],
    ) -> None:
        producer, native, _ = delivery
        future = await producer.send("topic", value=b"payload", no_confirm=True)
        assert isinstance(future, asyncio.Future)
        acknowledge = native.produce.call_args.kwargs["on_delivery"]
        error = KafkaError(KafkaError._MSG_TIMED_OUT)

        message = Mock(spec=Message)
        message_ref = ref(message)
        acknowledge(error, message)
        del message
        # Check the reference before allowing the delivery callback to run.
        message_released = message_ref() is None

        with pytest.raises(KafkaException) as exc_info:
            await future
        assert exc_info.value.args == (error,)
        assert message_released
