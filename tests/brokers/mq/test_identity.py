import pytest

from faststream.mq.helpers.client import (
    AsyncMQConnection,
    MQConnectionConfig,
    _build_raw_message,
)
from faststream.mq.response import MQPublishCommand
from faststream.response.publish_type import PublishType

VALID_MESSAGE_ID = "aa" * 24
VALID_CORRELATION_ID = "bb" * 24
FALLBACK_MESSAGE_ID = "cc" * 24
FALLBACK_CORRELATION_ID = "dd" * 24


@pytest.mark.mq()
def test_publish_command_rejects_non_hex_ids() -> None:
    with pytest.raises(ValueError, match="correlation_id"):
        MQPublishCommand(
            "hello",
            destination="DEV.QUEUE.1",
            correlation_id="1",
            _publish_type=PublishType.PUBLISH,
        )

    with pytest.raises(ValueError, match="message_id"):
        MQPublishCommand(
            "hello",
            destination="DEV.QUEUE.1",
            message_id="1",
            _publish_type=PublishType.PUBLISH,
        )


@pytest.mark.mq()
def test_publish_uses_mqmd_identity_fields(monkeypatch) -> None:
    property_names: list[str] = []
    published_md = None

    class FakeProperties:
        def set(self, name, value) -> None:
            property_names.append(name)

    class FakeMessageHandle:
        def __init__(self, qmgr) -> None:
            self.msg_handle = object()
            self.properties = FakeProperties()

        def dlt(self) -> None:
            return None

    class FakeMD:
        def __init__(self, Version=None) -> None:
            self.MsgId = None
            self.CorrelId = None
            self.ReplyToQ = ""
            self.ReplyToQMgr = ""
            self.Priority = None
            self.Persistence = None
            self.Expiry = None

    class FakePMO:
        def __init__(self, Version=None) -> None:
            self.OriginalMsgHandle = None

    class FakeQueue:
        def __init__(self, qmgr, target, open_opts) -> None:
            self.target = target

        def put(self, body, md, pmo) -> None:
            nonlocal published_md
            published_md = md

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
        "hello",
        destination="DEV.QUEUE.1",
        message_id=VALID_MESSAGE_ID,
        correlation_id=VALID_CORRELATION_ID,
        headers={"custom": "x"},
        _publish_type=PublishType.PUBLISH,
    )

    connection._publish_sync(cmd, serializer=None)

    assert published_md is not None
    assert published_md.MsgId == bytes.fromhex(VALID_MESSAGE_ID)
    assert published_md.CorrelId == bytes.fromhex(VALID_CORRELATION_ID)
    assert "usr.message_id" not in property_names
    assert "usr.correlation_id" not in property_names
    assert "usr.custom" in property_names


@pytest.mark.mq()
def test_parse_prefers_mqmd_identity_fields() -> None:
    class FakeMD:
        MsgId = bytes.fromhex(VALID_MESSAGE_ID)
        CorrelId = bytes.fromhex(VALID_CORRELATION_ID)
        ReplyToQ = ""
        ReplyToQMgr = ""
        Priority = None
        Persistence = None
        Expiry = None

    message = _build_raw_message(
        body=b"hello",
        md=FakeMD(),
        headers={
            "message_id": FALLBACK_MESSAGE_ID,
            "correlation_id": FALLBACK_CORRELATION_ID,
            "custom": "x",
        },
        queue_name="DEV.QUEUE.1",
        connection=None,
    )

    assert message.message_id == VALID_MESSAGE_ID
    assert message.correlation_id == VALID_CORRELATION_ID
    assert message.headers == {"custom": "x"}
