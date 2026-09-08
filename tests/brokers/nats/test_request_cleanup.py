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

pytestmark = [pytest.mark.asyncio(), pytest.mark.nats()]

RequestTransport: TypeAlias = tuple[NatsBroker, Client, list[bytes]]
RealRequestTransport: TypeAlias = tuple[NatsBroker, Client, set[int]]


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


@pytest_asyncio.fixture()
async def real_request_transport(queue: str) -> AsyncIterator[RealRequestTransport]:
    async with NatsBroker() as broker:
        connection = broker._connection
        assert connection is not None
        jetstream = connection.jetstream()
        await jetstream.add_stream(name=queue, subjects=[queue])
        try:
            # Creating the stream also initializes the shared JetStream reply mux.
            yield broker, connection, set(connection._subs)
        finally:
            await jetstream.delete_stream(queue)


async def test_timed_out_jetstream_request_removes_reply_subscription(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, commands = request_transport
    monkeypatch.setattr(
        JetStreamContext, "publish", AsyncMock(return_value=PubAck(stream="jobs", seq=1))
    )

    with pytest.raises(TimeoutError):
        await broker.request("request", "jobs", stream="jobs", timeout=0.01)

    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


async def test_failed_jetstream_publish_removes_reply_subscription(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, commands = request_transport
    monkeypatch.setattr(
        JetStreamContext, "publish", AsyncMock(side_effect=nats.errors.NoRespondersError)
    )

    with pytest.raises(nats.errors.NoRespondersError):
        await broker.request("request", "jobs", stream="jobs", timeout=0.01)

    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


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

    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


@pytest.mark.connected()
async def test_real_timed_out_requests_remove_reply_subscriptions(
    real_request_transport: RealRequestTransport,
    queue: str,
) -> None:
    broker, connection, subscriptions = real_request_transport
    for _ in range(3):
        with pytest.raises(TimeoutError):
            await broker.request("request", queue, stream=queue, timeout=0.05)

        await connection.flush()
        assert set(connection._subs) == subscriptions


@pytest.mark.connected()
async def test_real_failed_publishes_remove_reply_subscriptions(
    real_request_transport: RealRequestTransport,
    queue: str,
) -> None:
    broker, connection, subscriptions = real_request_transport
    for _ in range(3):
        with pytest.raises(nats.js.errors.NoStreamResponseError):
            await broker.request("request", f"{queue}.missing", stream=queue, timeout=3)

        await connection.flush()
        assert set(connection._subs) == subscriptions


@pytest.mark.connected()
async def test_real_cancelled_requests_remove_reply_subscriptions(
    real_request_transport: RealRequestTransport,
    queue: str,
) -> None:
    broker, connection, subscriptions = real_request_transport
    for _ in range(3):
        with anyio.move_on_after(0.05) as scope:
            await broker.request("request", queue, stream=queue, timeout=10)
        assert scope.cancelled_caught

        await connection.flush()
        assert set(connection._subs) == subscriptions


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

    assert scope.cancelled_caught
    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


async def test_successful_jetstream_request_keeps_reply(
    request_transport: RequestTransport,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker, connection, _ = request_transport

    async def publish(*args: object, headers: dict[str, str], **kwargs: object) -> PubAck:
        inbox = headers["reply_to"]
        await connection._process_msg(1, inbox.encode(), b"", b"response", b"")
        return PubAck(stream="jobs", seq=1)

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    response = await broker.request("request", "jobs", stream="jobs")

    assert response.body == b"response"
    assert not connection._subs


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
