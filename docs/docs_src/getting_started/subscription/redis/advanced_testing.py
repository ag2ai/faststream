from typing import Annotated

import pytest
from dirty_equals import IsPartialDict
from pydantic import BaseModel

from faststream import FastStream, Header
from faststream.redis import RedisBroker, TestRedisBroker

broker = RedisBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test-channel")
async def handle(
    data: Data,
    trace_id: Annotated[str, Header("trace-id")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert trace_id == "42"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            channel="test-channel",
            headers={"trace-id": "42"},
        )

        await handle.assert_called_once_with(
            {"name": "John", "user_id": 1},
            headers={"trace-id": "42"},
        )
        # or
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            headers={"trace-id": "42"},
        )


@pytest.mark.asyncio
async def test_message_context() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            channel="test-channel",
            headers={"trace-id": "42"},
        )

        await handle.assert_called_once_with(
            IsPartialDict(name="John"),
            context={"message.raw_message.channel": "test-channel"},
        )


@pytest.mark.asyncio
async def test_several_messages() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            channel="test-channel",
            headers={"trace-id": "42"},
            correlation_id="first",
        )
        await br.publish(
            Data(name="John", user_id=1),
            channel="test-channel",
            headers={"trace-id": "42"},
            correlation_id="second",
        )

        # the last message, as `mock.assert_called_with` reads it
        await handle.assert_called_with(
            Data(name="John", user_id=1),
            correlation_id="second",
        )
        # any of the messages
        await handle.assert_any_call(
            Data(name="John", user_id=1),
            correlation_id="first",
        )


@pytest.mark.asyncio
async def test_redis_fields() -> None:
    async with TestRedisBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            channel="test-channel",
            headers={"trace-id": "42"},
        )

        # `channel`, `list` and `stream` are named as `publish()` names them
        await handle.assert_called_once_with(
            Data(name="John", user_id=1),
            channel="test-channel",
        )
