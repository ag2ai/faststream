from collections.abc import AsyncIterator
from typing import Any

import anyio
import pytest

from faststream._internal._compat import ExceptionGroup
from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream.message import StreamMessage


class _RecordingSubscriber(SubscriberUsecase[Any]):
    def __init__(self, events: list[str]) -> None:
        self.events = events

    async def stop(self) -> None:
        self.events.append("enter")
        # a shutdown that awaits one subscriber at a time has nothing else to run here
        await anyio.lowlevel.checkpoint()
        self.events.append("exit")

    def _make_response_publisher(self, message: Any) -> Any:
        raise NotImplementedError

    async def get_one(self, *, timeout: float = 5) -> StreamMessage[Any] | None:
        raise NotImplementedError

    def __aiter__(self) -> AsyncIterator[StreamMessage[Any]]:
        raise NotImplementedError


class _StoppingBroker(BrokerUsecase[Any, Any, Any]):
    def __init__(self, subscribers: list[SubscriberUsecase[Any]]) -> None:
        self._stopping = subscribers
        self.running = True

    @property
    def subscribers(self) -> list[SubscriberUsecase[Any]]:
        return self._stopping

    async def _connect(self) -> Any:
        raise NotImplementedError

    async def ping(self, timeout: float | None) -> bool:
        raise NotImplementedError

    def subscriber(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    def publisher(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def publish(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError

    async def request(self, *args: Any, **kwargs: Any) -> Any:
        raise NotImplementedError


@pytest.mark.asyncio()
async def test_subscribers_stop_together() -> None:
    events: list[str] = []
    broker = _StoppingBroker([_RecordingSubscriber(events) for _ in range(3)])

    await broker.stop()

    assert events == ["enter", "enter", "enter", "exit", "exit", "exit"]
    assert not broker.running


class _FailingSubscriber(_RecordingSubscriber):
    async def stop(self) -> None:
        self.events.append("fail")
        msg = "stop failed"
        raise RuntimeError(msg)


@pytest.mark.asyncio()
async def test_failing_subscriber_does_not_prevent_others_from_stopping() -> None:
    events: list[str] = []
    broker = _StoppingBroker([
        _FailingSubscriber(events),
        *(_RecordingSubscriber(events) for _ in range(2)),
    ])

    with pytest.raises(ExceptionGroup) as exc_info:
        await broker.stop()

    assert events.count("exit") == 2
    assert [str(e) for e in exc_info.value.exceptions] == ["stop failed"]
    assert not broker.running
