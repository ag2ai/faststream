import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from faststream.confluent.helpers.client import AsyncConfluentProducer


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_send_delivery_after_cancellation_does_not_raise() -> None:
    """Fixes https://github.com/airtai/faststream/issues/3259.

    Delivery report arriving after the awaiting task is cancelled should not
    raise InvalidStateError on the event loop.
    """
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    orig_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: loop_errors.append(ctx))

    native = MagicMock()
    delivered_callback: Any = None

    def capture_produce(_topic: str, **kwargs: Any) -> None:
        nonlocal delivered_callback
        delivered_callback = kwargs["on_delivery"]

    native.produce.side_effect = capture_produce
    config: Any = MagicMock()
    logger: Any = MagicMock()

    try:
        with patch("faststream.confluent.helpers.client.Producer", return_value=native):
            producer = AsyncConfluentProducer(logger=logger, config=config)
            try:
                task = asyncio.create_task(producer.send("topic", value=b"payload"))
                await asyncio.sleep(0)  # parked on await result_future

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                mock_msg = MagicMock()
                mock_msg.error.return_value = None
                delivered_callback(None, mock_msg)
                await asyncio.sleep(0)  # run scheduled callbacks on event loop
            finally:
                await producer.stop()
    finally:
        loop.set_exception_handler(orig_handler)

    assert loop_errors == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_send_delivery_error_after_cancellation_does_not_raise() -> None:
    """Fixes https://github.com/airtai/faststream/issues/3259.

    Delivery error arriving after the awaiting task is cancelled should not
    raise InvalidStateError on the event loop.
    """
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    orig_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: loop_errors.append(ctx))

    native = MagicMock()
    delivered_callback: Any = None

    def capture_produce(_topic: str, **kwargs: Any) -> None:
        nonlocal delivered_callback
        delivered_callback = kwargs["on_delivery"]

    native.produce.side_effect = capture_produce
    config: Any = MagicMock()
    logger: Any = MagicMock()

    try:
        with patch("faststream.confluent.helpers.client.Producer", return_value=native):
            producer = AsyncConfluentProducer(logger=logger, config=config)
            try:
                task = asyncio.create_task(producer.send("topic", value=b"payload"))
                await asyncio.sleep(0)

                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task

                mock_err = MagicMock(spec=KafkaError)
                delivered_callback(mock_err, None)
                await asyncio.sleep(0)
            finally:
                await producer.stop()
    finally:
        loop.set_exception_handler(orig_handler)

    assert loop_errors == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_send_duplicate_delivery_report() -> None:
    """Duplicate on_delivery calls must not raise InvalidStateError."""
    loop_errors: list[dict[str, Any]] = []
    loop = asyncio.get_running_loop()
    orig_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, ctx: loop_errors.append(ctx))

    native = MagicMock()
    delivered_callback: Any = None

    def capture_produce(_topic: str, **kwargs: Any) -> None:
        nonlocal delivered_callback
        delivered_callback = kwargs["on_delivery"]

    native.produce.side_effect = capture_produce
    config: Any = MagicMock()
    logger: Any = MagicMock()

    try:
        with patch("faststream.confluent.helpers.client.Producer", return_value=native):
            producer = AsyncConfluentProducer(logger=logger, config=config)
            try:
                task = asyncio.create_task(producer.send("topic", value=b"payload"))
                await asyncio.sleep(0)

                mock_msg = MagicMock()
                mock_msg.error.return_value = None

                # First delivery report completes the future
                delivered_callback(None, mock_msg)
                result = await task
                assert result is mock_msg

                # Second (duplicate) delivery report
                delivered_callback(None, mock_msg)
                await asyncio.sleep(0)
            finally:
                await producer.stop()
    finally:
        loop.set_exception_handler(orig_handler)

    assert loop_errors == []


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_producer_send_normal_success_and_error() -> None:
    """Normal produce success and error paths continue working as expected."""
    native = MagicMock()

    config: Any = MagicMock()
    logger: Any = MagicMock()

    mock_msg = MagicMock()
    mock_msg.error.return_value = None

    def ack_success(_topic: str, **kwargs: Any) -> None:
        kwargs["on_delivery"](None, mock_msg)

    native.produce.side_effect = ack_success

    with patch("faststream.confluent.helpers.client.Producer", return_value=native):
        producer = AsyncConfluentProducer(logger=logger, config=config)
        try:
            res = await producer.send("topic", value=b"payload")
            assert res is mock_msg
        finally:
            await producer.stop()

    mock_err = MagicMock(spec=KafkaError)

    def ack_failure(_topic: str, **kwargs: Any) -> None:
        kwargs["on_delivery"](mock_err, None)

    native.produce.side_effect = ack_failure

    with patch("faststream.confluent.helpers.client.Producer", return_value=native):
        producer = AsyncConfluentProducer(logger=logger, config=config)
        try:
            with pytest.raises(KafkaException):
                await producer.send("topic", value=b"payload")
        finally:
            await producer.stop()
