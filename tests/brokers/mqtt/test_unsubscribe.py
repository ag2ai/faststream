import asyncio
from typing import Any

import pytest
import zmqtt

from faststream.mqtt import MQTTBroker


class _RecordingSubscription:
    def __init__(self) -> None:
        self.stop_calls = 0

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        self.stop_calls += 1

    def __aiter__(self) -> "_RecordingSubscription":
        return self

    async def __anext__(self) -> zmqtt.Message:
        # Block until the consume task is cancelled by subscriber.stop()
        await asyncio.sleep(1e9)
        raise StopAsyncIteration  # pragma: no cover


class _FakeClient:
    def __init__(self) -> None:
        self.subscriptions: list[_RecordingSubscription] = []

    def subscribe(self, *args: Any, **kwargs: Any) -> _RecordingSubscription:
        subscription = _RecordingSubscription()
        self.subscriptions.append(subscription)
        return subscription

    async def disconnect(self) -> None:
        pass


def _patch_connection(
    broker: MQTTBroker,
    monkeypatch: pytest.MonkeyPatch,
) -> _FakeClient:
    client = _FakeClient()

    async def connect() -> _FakeClient:
        broker.config.connect(client)
        return client

    monkeypatch.setattr(broker, "_connect", connect)
    return client


def _build_broker(
    monkeypatch: pytest.MonkeyPatch,
    topic: str,
    **kwargs: Any,
) -> tuple[MQTTBroker, _FakeClient]:
    broker = MQTTBroker(**kwargs)
    subscriber = broker.subscriber(topic)

    @subscriber
    async def handle() -> None:
        pass

    return broker, _patch_connection(broker, monkeypatch)


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_broker_shutdown_keeps_subscription(
    monkeypatch: pytest.MonkeyPatch,
    queue: str,
) -> None:
    broker, client = _build_broker(monkeypatch, queue, clean_session=False)

    await broker.start()
    await broker.stop()

    assert [s.stop_calls for s in client.subscriptions] == [0]


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_explicit_subscriber_stop_unsubscribes(
    monkeypatch: pytest.MonkeyPatch,
    queue: str,
) -> None:
    broker, client = _build_broker(monkeypatch, queue, clean_session=False)

    await broker.start()
    await broker.subscribers[0].stop()
    await broker.stop()

    assert [s.stop_calls for s in client.subscriptions] == [1]


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_restart_after_shutdown_unsubscribes_again(
    monkeypatch: pytest.MonkeyPatch,
    queue: str,
) -> None:
    broker, client = _build_broker(monkeypatch, queue, clean_session=False)

    await broker.start()
    await broker.stop()
    await broker.start()
    await broker.subscribers[0].stop()

    # the shutdown flag is reset by start(), so the explicit stop unsubscribes
    assert [s.stop_calls for s in client.subscriptions] == [0, 1]
