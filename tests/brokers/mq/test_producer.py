import pytest

from faststream.mq.helpers.client import MQConnectionConfig
from faststream.mq.publisher.producer import AsyncMQFastProducerImpl
from faststream.mq.response import MQPublishCommand
from faststream.response.publish_type import PublishType


@pytest.mark.mq()
@pytest.mark.asyncio()
async def test_request_uses_dedicated_connection(monkeypatch) -> None:
    events: list[tuple[str, str]] = []

    class DummyConnection:
        counter = 0

        def __init__(self, *, connection_config) -> None:
            type(self).counter += 1
            self.name = f"conn-{self.counter}"
            self.connection_config = connection_config

        async def connect(self) -> None:
            events.append((self.name, "connect"))

        async def disconnect(self) -> None:
            events.append((self.name, "disconnect"))

        async def ping(self, timeout: float) -> bool:
            events.append((self.name, "ping"))
            return True

        async def publish(self, cmd, *, serializer) -> None:
            events.append((self.name, "publish"))

        async def request(self, cmd, *, serializer):
            events.append((self.name, "request"))
            return cmd.destination

    monkeypatch.setattr(
        "faststream.mq.helpers.client.AsyncMQConnection",
        DummyConnection,
    )

    producer = AsyncMQFastProducerImpl(parser=None, decoder=None)
    await producer.connect(
        connection_config=MQConnectionConfig(
            queue_manager="QM1",
            channel="DEV.APP.SVRCONN",
            conn_name="127.0.0.1(1414)",
        ),
        serializer=None,
    )

    cmd = MQPublishCommand(
        "hello",
        destination="DEV.QUEUE.1",
        _publish_type=PublishType.PUBLISH,
    )

    await producer.publish(cmd)
    await producer.request(cmd)
    await producer.request(cmd)
    await producer.ping(1.0)
    await producer.disconnect()

    assert events == [
        ("conn-1", "connect"),
        ("conn-1", "publish"),
        ("conn-2", "connect"),
        ("conn-2", "request"),
        ("conn-2", "request"),
        ("conn-1", "ping"),
        ("conn-1", "disconnect"),
        ("conn-2", "disconnect"),
    ]
