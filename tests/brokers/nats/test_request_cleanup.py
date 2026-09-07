from unittest.mock import AsyncMock

import anyio
import nats
import pytest
import pytest_asyncio
from nats.js import JetStreamContext
from nats.js.api import PubAck

from faststream.nats import NatsBroker

pytestmark = [pytest.mark.asyncio(), pytest.mark.nats()]


@pytest_asyncio.fixture()
async def request_transport(monkeypatch):
    connection = nats.NATS()
    commands = []

    async def send_command(command):
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


@pytest.mark.parametrize("failure", ("timeout", "publish", "unsubscribe"))
async def test_failed_jetstream_request_removes_reply_subscription(
    request_transport,
    monkeypatch,
    failure: str,
) -> None:
    broker, connection, commands = request_transport
    publish = AsyncMock(return_value=PubAck(stream="jobs", seq=1))
    monkeypatch.setattr(JetStreamContext, "publish", publish)
    expected_error = TimeoutError
    if failure == "publish":
        publish.side_effect = nats.errors.NoRespondersError
        expected_error = nats.errors.NoRespondersError
    elif failure == "unsubscribe":
        send_unsubscribe = connection._send_unsubscribe

        async def fail_initial_unsubscribe(sid, limit=0):
            if limit:
                raise RuntimeError
            await send_unsubscribe(sid, limit)

        monkeypatch.setattr(connection, "_send_unsubscribe", fail_initial_unsubscribe)
        expected_error = RuntimeError

    with pytest.raises(expected_error):
        await broker.request("request", "jobs", stream="jobs", timeout=0.01)

    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


@pytest.mark.connected()
@pytest.mark.parametrize("failure", ("timeout", "publish", "cancel"))
async def test_real_failed_jetstream_requests_remove_reply_subscriptions(
    queue: str,
    failure: str,
) -> None:
    async with NatsBroker() as broker:
        connection = broker._connection
        jetstream = connection.jetstream()
        await jetstream.add_stream(name=queue, subjects=[queue])
        try:
            # Creating the stream also initializes the shared JetStream reply mux.
            subscriptions = set(connection._subs)
            for _ in range(3):
                if failure == "cancel":
                    with anyio.move_on_after(0.05) as scope:
                        await broker.request("request", queue, stream=queue, timeout=10)
                    assert scope.cancelled_caught
                else:
                    subject = queue if failure == "timeout" else f"{queue}.missing"
                    expected_error = (
                        TimeoutError
                        if failure == "timeout"
                        else nats.js.errors.NoStreamResponseError
                    )
                    with pytest.raises(expected_error):
                        await broker.request(
                            "request",
                            subject,
                            stream=queue,
                            timeout=0.05 if failure == "timeout" else 3,
                        )

                await connection.flush()
                assert set(connection._subs) == subscriptions
        finally:
            await jetstream.delete_stream(queue)


async def test_cancelled_jetstream_request_unsubscribes_under_cancel_scope(
    request_transport,
    monkeypatch,
) -> None:
    broker, connection, commands = request_transport

    async def publish(*args, **kwargs):
        scope.cancel()
        await anyio.sleep_forever()

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    with anyio.CancelScope() as scope:
        await broker.request("request", "jobs", stream="jobs")

    assert scope.cancelled_caught
    assert not connection._subs
    assert commands[-1].split() == [b"UNSUB", b"1"]


async def test_successful_jetstream_request_keeps_reply(
    request_transport,
    monkeypatch,
) -> None:
    broker, connection, _ = request_transport

    async def publish(*args, **kwargs):
        inbox = kwargs["headers"]["reply_to"]
        await connection._process_msg(1, inbox.encode(), b"", b"response", None)
        return PubAck(stream="jobs", seq=1)

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    response = await broker.request("request", "jobs", stream="jobs")

    assert response.body == b"response"
    assert not connection._subs


async def test_closed_connection_preserves_publish_error(
    request_transport,
    monkeypatch,
) -> None:
    broker, connection, _ = request_transport

    async def publish(*args, **kwargs):
        await connection.close()
        raise nats.errors.TimeoutError

    monkeypatch.setattr(JetStreamContext, "publish", publish)
    with pytest.raises(nats.errors.TimeoutError):
        await broker.request("request", "jobs", stream="jobs")

    assert not connection._subs
