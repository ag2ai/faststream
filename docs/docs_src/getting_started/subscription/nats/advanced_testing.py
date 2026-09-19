from typing import Annotated

import pytest
from dirty_equals import IsPartialDict
from pydantic import BaseModel

from faststream import FastStream, Header
from faststream.nats import NatsBroker, TestNatsBroker

broker = NatsBroker()
app = FastStream(broker)


class Data(BaseModel):
    name: str
    user_id: int


@broker.subscriber("test.subject")
async def handle(
    data: Data,
    trace_id: Annotated[str, Header("trace-id")],
) -> None:
    assert data.name == "John"
    assert data.user_id == 1
    assert trace_id == "42"


@pytest.mark.asyncio
async def test_handle() -> None:
    async with TestNatsBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            subject="test.subject",
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
    async with TestNatsBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            subject="test.subject",
            headers={"trace-id": "42"},
        )

        await handle.assert_called_once_with(
            IsPartialDict(name="John"),
            context={"message.raw_message.subject": "test.subject"},
        )


@pytest.mark.asyncio
async def test_several_messages() -> None:
    async with TestNatsBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            subject="test.subject",
            headers={"trace-id": "42"},
            correlation_id="first",
        )
        await br.publish(
            Data(name="John", user_id=1),
            subject="test.subject",
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


@broker.subscriber("logs.{level}")
async def handle_logs(data: Data) -> None:
    assert data.name == "John"


@pytest.mark.asyncio
async def test_nats_fields() -> None:
    async with TestNatsBroker(broker) as br:
        await br.publish(
            Data(name="John", user_id=1),
            subject="logs.info",
        )

        # `path` holds what the template captured, `subject` the whole address
        await handle_logs.assert_called_once_with(
            Data(name="John", user_id=1),
            path={"level": "info"},
            subject="logs.info",
        )
