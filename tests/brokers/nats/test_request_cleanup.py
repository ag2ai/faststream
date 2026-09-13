from collections.abc import AsyncIterator
from typing import TypeAlias
from unittest.mock import AsyncMock

import anyio
import nats
import pytest
import pytest_asyncio
from nats.aio.client import Client
from nats.js import JetStreamContext
from nats.js.api import PubAck

from faststream.nats import NatsBroker

from .settings import Settings

pytestmark = [pytest.mark.asyncio(), pytest.mark.nats()]

RequestTransport: TypeAlias = tuple[NatsBroker, Client, list[bytes]]


@pytest_asyncio.fixture()
async def request_transport(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[RequestTransport]:
    connection = Client()
    commands: list[bytes] = []

    async def send_command(command: bytes) -> None:
        await anyio.lowlevel.checkpoint()
        commands.append(command)

    monkeypatch.setattr(connection, "_send_command", send_command)
    monkeypatch.setattr(connection, "_flush_pending", AsyncMock())
    monkeypatch.setattr(nats, "connect", AsyncMock(return_value=connection))
    broker = NatsBroker()
    await broker.connect()
    try:
        yield broker, connection, commands
    finally:
        await connection.close()


async def test_failed_reply_limit_setup_removes_reply_subscription(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, commands = request_transport
    send_unsubscribe = connection._send_unsubscribe

    async def fail_initial_unsubscribe(sid: int, limit: int = 0) -> None:
        if limit:
            raise RuntimeError
        await send_unsubscribe(sid, limit)

    monkeypatch.setattr(connection, "_send_unsubscribe", fail_initial_unsubscribe)

    with pytest.raises(RuntimeError):
        await broker.request("request", "jobs", stream="jobs", timeout=0.01)

    assert (connection._subs, commands[-1].split()) == ({}, [b"UNSUB", b"1"])


@pytest.mark.connected()
async def test_real_timed_out_requests_remove_reply_subscriptions(
    settings: Settings,
    queue: str,
) -> None:
    async with NatsBroker(settings.url) as broker:
        connection = broker._connection
        assert connection is not None
        jetstream = connection.jetstream()
        await jetstream.add_stream(name=queue, subjects=[queue])
        try:
            # Creating the stream first includes the shared JetStream reply mux.
            subscriptions = set(connection._subs)
            with pytest.raises(TimeoutError):
                await broker.request("request", queue, stream=queue, timeout=0.05)

            await connection.flush()
            assert set(connection._subs) == subscriptions
        finally:
            await jetstream.delete_stream(queue)


async def test_cancelled_jetstream_request_unsubscribes_under_cancel_scope(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, commands = request_transport

    async def publish(*args: object, **kwargs: object) -> None:
        scope.cancel()
        await anyio.sleep_forever()

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    with anyio.CancelScope() as scope:
        await broker.request("request", "jobs", stream="jobs")

    assert (scope.cancelled_caught, connection._subs, commands[-1].split()) == (
        True,
        {},
        [b"UNSUB", b"1"],
    )


async def test_closed_connection_preserves_publish_error(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, _ = request_transport

    async def publish(*args: object, **kwargs: object) -> PubAck:
        await connection.close()
        raise nats.errors.TimeoutError

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    with pytest.raises(nats.errors.TimeoutError):
        await broker.request("request", "jobs", stream="jobs")

    assert not connection._subs
