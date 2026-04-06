from __future__ import annotations

import asyncio
from queue import Queue as ThreadQueue

import pytest

from faststream import BaseMiddleware
from tests.marks import require_ibmmq

from .basic import MQTestcaseConfig
from .utils import MQAdminConfig, delete_queue, ensure_queue, get_queue_depth

VALID_MESSAGE_ID = "11" * 24
VALID_CORRELATION_ID = "22" * 24


def _read_headers(mq, msg_handle) -> dict[str, object]:
    headers: dict[str, object] = {}
    options = mq.CMQC.MQIMPO_INQ_FIRST

    while True:
        try:
            value, property_name = msg_handle.properties.get(
                "usr.%",
                impo_options=options,
            )
        except mq.MQMIError as e:
            if e.reason == mq.CMQC.MQRC_PROPERTY_NOT_AVAILABLE:
                break
            raise

        if isinstance(property_name, bytes):
            name = property_name.decode().strip()
        else:
            name = str(property_name).strip()
        if name.startswith("usr."):
            name = name[4:]
        headers[name.replace("_dash_", "-")] = value
        options = mq.CMQC.MQIMPO_INQ_NEXT

    return headers


def _mq_str(value: object) -> str:
    if isinstance(value, bytes):
        return value.decode().strip()
    return str(value).strip()


def _consume_request_and_reply(
    queue_name: str,
    admin_config: MQAdminConfig,
    results: ThreadQueue,
) -> None:
    import ibmmq as mq

    qmgr = mq.QueueManager(None)
    request_queue = None
    reply_queue = None
    request_handle = None

    try:
        qmgr.connect_tcp_client(
            admin_config.queue_manager,
            mq.CD(),
            "DEV.APP.SVRCONN",
            admin_config.conn_name,
            "app",
            "password",
        )
        request_queue = mq.Queue(qmgr, queue_name, mq.CMQC.MQOO_INPUT_AS_Q_DEF)

        gmo = mq.GMO(Version=mq.CMQC.MQGMO_CURRENT_VERSION)
        gmo.Options = mq.CMQC.MQGMO_WAIT | mq.CMQC.MQGMO_PROPERTIES_IN_HANDLE
        gmo.WaitInterval = 5000

        request_handle = mq.MessageHandle(qmgr)
        gmo.MsgHandle = request_handle.msg_handle

        md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
        body = request_queue.get(None, md, gmo)
        headers = _read_headers(mq, request_handle)

        results.put(
            {
                "body": body,
                "msg_id": bytes(md.MsgId),
                "correl_id": bytes(md.CorrelId),
                "reply_to": _mq_str(md.ReplyToQ),
                "reply_to_qmgr": _mq_str(md.ReplyToQMgr),
                "expiry": md.Expiry,
                "headers": headers,
            },
        )

        reply_queue = mq.Queue(qmgr, md.ReplyToQ, mq.CMQC.MQOO_OUTPUT)
        reply_md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
        reply_md.CorrelId = md.MsgId
        reply_queue.put(b"response", reply_md, mq.PMO(Version=mq.CMQC.MQPMO_VERSION_3))

    finally:
        if reply_queue is not None:
            reply_queue.close()
        if request_queue is not None:
            request_queue.close()
        if request_handle is not None:
            request_handle.dlt()
        if qmgr.is_connected:
            qmgr.disconnect()


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
@pytest.mark.asyncio()
class TestConnectedRegressions(MQTestcaseConfig):
    async def test_request_writes_expected_mqmd_metadata(self, queue: str) -> None:
        admin_config = MQAdminConfig()
        ensure_queue(queue, admin_config)
        broker = self.get_broker()
        results: ThreadQueue = ThreadQueue()

        try:
            responder = asyncio.create_task(
                asyncio.to_thread(
                    _consume_request_and_reply,
                    queue,
                    admin_config,
                    results,
                ),
            )

            async with broker:
                await broker.start()
                response = await broker.request(
                    "payload",
                    queue,
                    timeout=self.timeout,
                    message_id=VALID_MESSAGE_ID,
                    correlation_id=VALID_CORRELATION_ID,
                    expiry=42,
                    headers={"source": "connected-test"},
                )

            await responder

            captured = results.get_nowait()
            assert await response.decode() == b"response"
            assert response.correlation_id == VALID_MESSAGE_ID
            assert captured == {
                "body": b"payload",
                "msg_id": bytes.fromhex(VALID_MESSAGE_ID),
                "correl_id": bytes.fromhex(VALID_CORRELATION_ID),
                "reply_to": captured["reply_to"],
                "reply_to_qmgr": "QM1",
                "expiry": 42,
                "headers": {"source": "connected-test", "content-type": "text/plain"},
            }
            assert captured["reply_to"].startswith("FASTSTREAM.REPLY.")

        finally:
            delete_queue(queue, admin_config)

    async def test_parser_failure_backouts_message_on_real_queue(
        self, queue: str
    ) -> None:
        admin_config = MQAdminConfig()
        ensure_queue(queue, admin_config)
        event = asyncio.Event()

        async def broken_parser(msg, original):
            event.set()
            raise ValueError("boom")

        broker = self.get_broker(parser=broken_parser)
        subscriber = broker.subscriber(queue)

        @subscriber
        async def handler(msg) -> None: ...

        try:
            async with broker:
                await broker.start()
                await broker.publish("hello", queue)
                await asyncio.wait_for(event.wait(), timeout=self.timeout)
                await asyncio.sleep(0.2)

            assert get_queue_depth(queue, admin_config) == 1

        finally:
            delete_queue(queue, admin_config)

    async def test_no_handler_failure_backouts_message_on_real_queue(
        self, queue: str
    ) -> None:
        admin_config = MQAdminConfig()
        ensure_queue(queue, admin_config)
        event = asyncio.Event()

        async def non_matching_filter(msg) -> bool:
            event.set()
            return msg.content_type == "application/json"

        broker = self.get_broker()
        subscriber = broker.subscriber(queue)

        @subscriber(filter=non_matching_filter)
        async def handler(msg) -> None: ...

        try:
            async with broker:
                await broker.start()
                await broker.publish("hello", queue)
                await asyncio.wait_for(event.wait(), timeout=self.timeout)
                await asyncio.sleep(0.2)

            assert get_queue_depth(queue, admin_config) == 1

        finally:
            delete_queue(queue, admin_config)

    async def test_reply_is_backed_out_with_failed_consume(self, queue: str) -> None:
        admin_config = MQAdminConfig()
        ensure_queue(queue, admin_config)
        event = asyncio.Event()

        class FailAfterProcessed(BaseMiddleware):
            async def after_processed(self, exc_type=None, exc_val=None, exc_tb=None):
                raise ValueError("boom")

        broker = self.get_broker(middlewares=(FailAfterProcessed,))

        @broker.subscriber(queue)
        async def handler(msg: str) -> str:
            event.set()
            return "response"

        try:
            async with broker:
                await broker.start()
                request_task = asyncio.create_task(
                    broker.request("hello", queue, timeout=1.0),
                )
                await asyncio.wait_for(event.wait(), timeout=self.timeout)
                await broker.stop()

                with pytest.raises(TimeoutError):
                    await request_task

            assert get_queue_depth(queue, admin_config) == 1

        finally:
            delete_queue(queue, admin_config)
