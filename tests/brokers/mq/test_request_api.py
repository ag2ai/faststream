import inspect

import pytest

from faststream.mq import MQBroker
from faststream.mq.helpers.client import AsyncMQConnection, MQConnectionConfig
from faststream.mq.publisher.usecase import MQPublisher
from faststream.mq.response import MQPublishCommand
from faststream.response.publish_type import PublishType


@pytest.mark.mq()
def test_request_signature_does_not_expose_reply_to() -> None:
    broker_signature = inspect.signature(MQBroker.request)
    publisher_signature = inspect.signature(MQPublisher.request)

    for name in ("reply_to", "reply_to_qmgr"):
        assert name not in broker_signature.parameters
        assert name not in publisher_signature.parameters

    assert "expiry" in broker_signature.parameters
    assert "expiry" in publisher_signature.parameters


@pytest.mark.mq()
def test_request_honors_expiry(monkeypatch) -> None:
    class FakeMQMIError(Exception):
        def __init__(self, comp: int, reason: int) -> None:
            self.comp = comp
            self.reason = reason

    class FakeProperties:
        def set(self, name, value) -> None:
            return None

        def get(self, pattern, impo_options):
            raise FakeMQMIError(2, FakeMQ.CMQC.MQRC_PROPERTY_NOT_AVAILABLE)

    class FakeMessageHandle:
        def __init__(self, qmgr) -> None:
            self.msg_handle = 1
            self.properties = FakeProperties()

        def dlt(self) -> None:
            return None

    class FakeMD:
        def __init__(self, Version=None) -> None:
            self.ReplyToQ = ""
            self.ReplyToQMgr = ""
            self.Priority = None
            self.Persistence = None
            self.Expiry = None
            self.CorrelId = None
            self.MsgId = b"request-id"

    class FakePMO:
        def __init__(self, Version=None) -> None:
            self.OriginalMsgHandle = None

    class FakeGMO:
        def __init__(self, Version=None) -> None:
            self.Options = 0
            self.MatchOptions = 0
            self.WaitInterval = 0
            self.MsgHandle = None

    class FakeOD:
        def __init__(self) -> None:
            self.ObjectName = "DEV.APP.MODEL.QUEUE"
            self.DynamicQName = ""

    published_md: FakeMD | None = None

    class FakeQueue:
        def __init__(self, qmgr, target, open_opts) -> None:
            if isinstance(target, FakeOD):
                target.ObjectName = "FASTSTREAM.REPLY.TEST"
                self.name = target.ObjectName
                self.is_reply = True
            else:
                self.name = target
                self.is_reply = False

        def put(self, body, md, pmo) -> None:
            nonlocal published_md
            published_md = md

        def get(self, body, md, gmo):
            md.MsgId = b"reply-id"
            md.CorrelId = b"request-id"
            return b"reply"

        def close(self) -> None:
            return None

    class FakeMQ:
        MQMIError = FakeMQMIError
        Queue = FakeQueue
        MessageHandle = FakeMessageHandle
        MD = FakeMD
        PMO = FakePMO
        GMO = FakeGMO
        OD = FakeOD

        class CMQC:
            MQMD_CURRENT_VERSION = 1
            MQPMO_VERSION_3 = 3
            MQGMO_CURRENT_VERSION = 1
            MQOO_INPUT_EXCLUSIVE = 1
            MQOO_OUTPUT = 2
            MQGMO_WAIT = 4
            MQGMO_NO_SYNCPOINT = 8
            MQGMO_PROPERTIES_IN_HANDLE = 16
            MQMO_MATCH_CORREL_ID = 32
            MQRC_NO_MSG_AVAILABLE = 2033
            MQRC_PROPERTY_NOT_AVAILABLE = 2492
            MQIMPO_INQ_FIRST = 1
            MQPER_PERSISTENT = 1
            MQPER_NOT_PERSISTENT = 0

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="127.0.0.1(1414)",
        ),
    )
    connection._qmgr = object()

    cmd = MQPublishCommand(
        "hello",
        destination="DEV.QUEUE.1",
        expiry=42,
        _publish_type=PublishType.PUBLISH,
    )

    result = connection._request_sync(cmd, serializer=None)

    assert published_md is not None
    assert published_md.Expiry == 42
    assert result.queue == "FASTSTREAM.REPLY.TEST"


@pytest.mark.mq()
def test_request_does_not_republish_after_post_put_failure(monkeypatch) -> None:
    class FakeMQMIError(Exception):
        def __init__(self, comp: int, reason: int) -> None:
            self.comp = comp
            self.reason = reason

    class FakeProperties:
        def set(self, name, value) -> None:
            return None

        def get(self, pattern, impo_options):
            raise FakeMQMIError(2, FakeMQ.CMQC.MQRC_PROPERTY_NOT_AVAILABLE)

    class FakeMessageHandle:
        def __init__(self, qmgr) -> None:
            self.msg_handle = 1
            self.properties = FakeProperties()

        def dlt(self) -> None:
            return None

    class FakeMD:
        def __init__(self, Version=None) -> None:
            self.ReplyToQ = ""
            self.ReplyToQMgr = ""
            self.Priority = None
            self.Persistence = None
            self.Expiry = None
            self.CorrelId = None
            self.MsgId = b"request-id"

    class FakePMO:
        def __init__(self, Version=None) -> None:
            self.OriginalMsgHandle = None

    class FakeGMO:
        def __init__(self, Version=None) -> None:
            self.Options = 0
            self.MatchOptions = 0
            self.WaitInterval = 0
            self.MsgHandle = None

    class FakeOD:
        def __init__(self) -> None:
            self.ObjectName = "DEV.APP.MODEL.QUEUE"
            self.DynamicQName = ""

    put_count = 0
    recoveries = 0

    class FakeQueue:
        def __init__(self, qmgr, target, open_opts) -> None:
            self.is_reply = isinstance(target, FakeOD)
            if self.is_reply:
                target.ObjectName = "FASTSTREAM.REPLY.TEST"

        def put(self, body, md, pmo) -> None:
            nonlocal put_count
            put_count += 1

        def get(self, body, md, gmo):
            raise FakeMQMIError(2, FakeMQ.CMQC.MQRC_CONNECTION_BROKEN)

        def close(self) -> None:
            return None

    class FakeMQ:
        MQMIError = FakeMQMIError
        Queue = FakeQueue
        MessageHandle = FakeMessageHandle
        MD = FakeMD
        PMO = FakePMO
        GMO = FakeGMO
        OD = FakeOD

        class CMQC:
            MQMD_CURRENT_VERSION = 1
            MQPMO_VERSION_3 = 3
            MQGMO_CURRENT_VERSION = 1
            MQOO_INPUT_EXCLUSIVE = 1
            MQOO_OUTPUT = 2
            MQGMO_WAIT = 4
            MQGMO_NO_SYNCPOINT = 8
            MQGMO_PROPERTIES_IN_HANDLE = 16
            MQMO_MATCH_CORREL_ID = 32
            MQRC_NO_MSG_AVAILABLE = 2033
            MQRC_PROPERTY_NOT_AVAILABLE = 2492
            MQRC_CONNECTION_BROKEN = 2009
            MQRC_HCONN_ERROR = 2018
            MQRC_Q_MGR_NOT_AVAILABLE = 2059
            MQRC_HOST_NOT_AVAILABLE = 2538
            MQRC_RECONNECTING = 2548
            MQRC_RECONNECTED = 2549
            MQRC_RECONNECT_FAILED = 2540
            MQRC_CALL_INTERRUPTED = 2530
            MQRC_RECONNECT_Q_MGR_REQD = 2556
            MQRC_RECONNECT_TIMED_OUT = 2557
            MQIMPO_INQ_FIRST = 1
            MQPER_PERSISTENT = 1
            MQPER_NOT_PERSISTENT = 0

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="127.0.0.1(1414)",
        ),
    )
    connection._qmgr = object()

    def fake_recover(*, reopen_consumer: bool) -> None:
        nonlocal recoveries
        recoveries += 1

    monkeypatch.setattr(connection, "_recover_connection_sync", fake_recover)

    cmd = MQPublishCommand(
        "hello",
        destination="DEV.QUEUE.1",
        _publish_type=PublishType.PUBLISH,
    )

    with pytest.raises(FakeMQMIError) as exc:
        connection._request_sync(cmd, serializer=None)

    assert exc.value.reason == FakeMQ.CMQC.MQRC_CONNECTION_BROKEN
    assert put_count == 1
    assert recoveries == 1


@pytest.mark.mq()
def test_reply_publish_uses_syncpoint_pmo(monkeypatch) -> None:
    published_pmo = None

    class FakeProperties:
        def set(self, name, value) -> None:
            return None

    class FakeMessageHandle:
        def __init__(self, qmgr) -> None:
            self.msg_handle = 1
            self.properties = FakeProperties()

        def dlt(self) -> None:
            return None

    class FakeMD:
        def __init__(self, Version=None) -> None:
            self.ReplyToQ = ""
            self.ReplyToQMgr = ""
            self.Priority = None
            self.Persistence = None
            self.Expiry = None
            self.CorrelId = None
            self.MsgId = b"request-id"

    class FakePMO:
        def __init__(self, Version=None) -> None:
            self.Options = 0
            self.OriginalMsgHandle = None

    class FakeQueue:
        def __init__(self, qmgr, target, open_opts) -> None:
            return None

        def put(self, body, md, pmo) -> None:
            nonlocal published_pmo
            published_pmo = pmo

        def close(self) -> None:
            return None

    class FakeMQ:
        Queue = FakeQueue
        MessageHandle = FakeMessageHandle
        MD = FakeMD
        PMO = FakePMO

        class CMQC:
            MQMD_CURRENT_VERSION = 1
            MQPMO_VERSION_3 = 3
            MQPMO_SYNCPOINT = 2
            MQOO_OUTPUT = 2
            MQPER_PERSISTENT = 1
            MQPER_NOT_PERSISTENT = 0

    monkeypatch.setattr("faststream.mq.helpers.client._load_ibmmq", lambda: FakeMQ)

    connection = AsyncMQConnection(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="127.0.0.1(1414)",
        ),
    )
    connection._qmgr = object()

    cmd = MQPublishCommand(
        "reply",
        destination="DEV.QUEUE.1",
        _publish_type=PublishType.REPLY,
        syncpoint=True,
    )

    connection._publish_sync(cmd, serializer=None)

    assert published_pmo is not None
    assert published_pmo.Options == FakeMQ.CMQC.MQPMO_SYNCPOINT
