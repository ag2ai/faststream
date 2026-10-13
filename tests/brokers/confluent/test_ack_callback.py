import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from faststream._internal.logger.state import LoggerState
from faststream.confluent.helpers.client import AsyncConfluentProducer
from faststream.confluent.helpers.config import ConfluentFastConfig


class FakeProducer:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.on_delivery: Any = None

    async def poll(self, timeout: float | None = None) -> int:
        await asyncio.sleep(0)
        return 0

    async def flush(self) -> int:
        return 0

    def produce(self, topic: str, **kwargs: Any) -> None:
        self.on_delivery = kwargs["on_delivery"]


@pytest.mark.confluent()
@pytest.mark.asyncio()
async def test_late_delivery_callback_ignores_cancelled_future() -> None:
    loop = asyncio.get_running_loop()
    original_handler = loop.get_exception_handler()
    exceptions: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: exceptions.append(context))

    try:
        with patch("faststream.confluent.helpers.client.Producer", FakeProducer):
            producer = AsyncConfluentProducer(
                logger=LoggerState(),
                config=ConfluentFastConfig(bootstrap_servers="localhost:9092"),
            )
            try:
                result = await producer.send("topic", value=b"value", no_confirm=True)
                assert isinstance(result, asyncio.Future)

                result.cancel()
                producer.producer.on_delivery(None, MagicMock(error=lambda: None))
                await asyncio.sleep(0)

                assert exceptions == []
            finally:
                await producer.stop()
    finally:
        loop.set_exception_handler(original_handler)
