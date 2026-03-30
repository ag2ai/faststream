from __future__ import annotations

import asyncio
from queue import Queue as ThreadQueue

import pytest

from faststream.mq import MQBroker
from faststream.mq.helpers.client import AsyncMQConnection, MQConnectionConfig
from tests.marks import require_ibmmq

from .utils import (
    MQHAConfig,
    delete_queue_on_many,
    ensure_queue_on_many,
    generated_ccdt,
    get_queue_depth,
    set_channel_state,
)


def _request_responder(queue_name: str, conn_name: str, results: ThreadQueue) -> None:
    import ibmmq as mq

    qmgr = mq.QueueManager(None)
    queue = None
    reply_queue = None

    try:
        qmgr.connect_tcp_client(
            "QM1",
            mq.CD(),
            "DEV.APP.SVRCONN",
            conn_name,
            "app",
            "password",
        )
        queue = mq.Queue(qmgr, queue_name, mq.CMQC.MQOO_INPUT_AS_Q_DEF)
        md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
        gmo = mq.GMO(Version=mq.CMQC.MQGMO_CURRENT_VERSION)
        gmo.Options = mq.CMQC.MQGMO_WAIT
        gmo.WaitInterval = 10000
        body = queue.get(None, md, gmo)
        results.put(body)

        reply_queue = mq.Queue(qmgr, md.ReplyToQ, mq.CMQC.MQOO_OUTPUT)
        reply_md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
        reply_md.CorrelId = md.MsgId
        reply_queue.put(b"reply:hello", reply_md, mq.PMO(Version=mq.CMQC.MQPMO_VERSION_3))

    finally:
        if reply_queue is not None:
            reply_queue.close()
        if queue is not None:
            queue.close()
        if qmgr.is_connected:
            qmgr.disconnect()


@pytest.mark.mq()
def test_ccdt_connection_uses_connect_with_options(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class FakeQueueManager:
        def connect_with_options(self, queue_manager, **kwargs) -> None:
            captured["queue_manager"] = queue_manager
            captured["kwargs"] = kwargs

    class FakeCNO:
        def __init__(self) -> None:
            self.Options = 0
            self.CCDTUrl = None

    class FakeMQ:
        QueueManager = lambda self=None: FakeQueueManager()
        CNO = FakeCNO

        class CMQC:
            MQCNO_RECONNECT_AS_DEF = 0
            MQCNO_RECONNECT = 1
            MQCNO_RECONNECT_DISABLED = 2
            MQCNO_RECONNECT_Q_MGR = 4

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            ccdt_url="file:///tmp/AMQCLCHL.TAB",
            reconnect_mode="qmgr",
            username="app",
            password="password",
        ),
    )

    connection._connect_sync()

    assert captured["queue_manager"] == "QM1"
    kwargs = captured["kwargs"]
    cno = kwargs["cno"]
    assert cno.CCDTUrl == "file:///tmp/AMQCLCHL.TAB"
    assert cno.Options == FakeMQ.CMQC.MQCNO_RECONNECT_Q_MGR
    assert kwargs["user"] == "app"
    assert kwargs["password"] == "password"


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
@pytest.mark.asyncio()
class TestHighAvailabilityReconnect:
    timeout = 15.0

    async def test_publish_recovers_after_primary_channel_stop(self, queue: str) -> None:
        ha = MQHAConfig()
        ensure_queue_on_many(queue, (ha.primary, ha.secondary))

        try:
            with generated_ccdt(ha) as ccdt_url:
                broker = MQBroker(
                    queue_manager="QM1",
                    ccdt_url=ccdt_url,
                    reconnect="qmgr",
                    username="app",
                    password="password",
                )

                async with broker:
                    await broker.start()
                    await broker.publish("one", queue)
                    set_channel_state(ha.channel, config=ha.primary, start=False)
                    await asyncio.sleep(1.0)
                    await broker.publish("two", queue)

                total_depth = get_queue_depth(queue, ha.primary) + get_queue_depth(
                    queue, ha.secondary
                )
                assert total_depth == 2

        finally:
            set_channel_state(ha.channel, config=ha.primary, start=True)
            delete_queue_on_many(queue, (ha.primary, ha.secondary))

    async def test_consumer_recovers_after_primary_channel_stop(self, queue: str) -> None:
        ha = MQHAConfig()
        ensure_queue_on_many(queue, (ha.primary, ha.secondary))
        event = asyncio.Event()
        seen: list[str] = []

        try:
            with generated_ccdt(ha) as ccdt_url:
                broker = MQBroker(
                    queue_manager="QM1",
                    ccdt_url=ccdt_url,
                    reconnect="qmgr",
                    username="app",
                    password="password",
                )

                @broker.subscriber(queue)
                async def handler(msg: str) -> None:
                    seen.append(msg)
                    event.set()

                async with broker:
                    await broker.start()
                    set_channel_state(ha.channel, config=ha.primary, start=False)
                    await asyncio.sleep(1.0)
                    await broker.publish("hello", queue)
                    await asyncio.wait_for(event.wait(), timeout=self.timeout)

                assert seen == ["hello"]

        finally:
            set_channel_state(ha.channel, config=ha.primary, start=True)
            delete_queue_on_many(queue, (ha.primary, ha.secondary))

    async def test_request_recovers_after_primary_channel_stop(self, queue: str) -> None:
        ha = MQHAConfig()
        ensure_queue_on_many(queue, (ha.primary, ha.secondary))
        results: ThreadQueue = ThreadQueue()

        try:
            with generated_ccdt(ha) as ccdt_url:
                broker = MQBroker(
                    queue_manager="QM1",
                    ccdt_url=ccdt_url,
                    reconnect="qmgr",
                    username="app",
                    password="password",
                )

                responder = asyncio.create_task(
                    asyncio.to_thread(
                        _request_responder,
                        queue,
                        ha.secondary.conn_name,
                        results,
                    ),
                )

                async with broker:
                    await broker.start()
                    set_channel_state(ha.channel, config=ha.primary, start=False)
                    await asyncio.sleep(1.0)
                    response = await broker.request("hello", queue, timeout=self.timeout)

                await responder
                assert results.get_nowait().endswith(b"hello")
                assert await response.decode() == b"reply:hello"

        finally:
            set_channel_state(ha.channel, config=ha.primary, start=True)
            delete_queue_on_many(queue, (ha.primary, ha.secondary))
