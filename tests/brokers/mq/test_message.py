import asyncio

import pytest

from faststream.mq.message import MQMessage, MQRawMessage


class CancelledConnection:
    async def commit(self) -> None:
        raise asyncio.CancelledError

    async def backout(self) -> None:
        raise asyncio.CancelledError


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestMQMessageSettlement:
    async def test_ack_reraises_cancellation_without_marking_message(self) -> None:
        message = MQMessage(
            raw_message=MQRawMessage(
                body=b"hello",
                queue="DEV.QUEUE.1",
                connection=CancelledConnection(),
            ),
            body=b"hello",
        )

        with pytest.raises(asyncio.CancelledError):
            await message.ack()

        assert message.committed is None
        assert message.raw_message.settled is None

    async def test_nack_reraises_cancellation_without_marking_message(self) -> None:
        message = MQMessage(
            raw_message=MQRawMessage(
                body=b"hello",
                queue="DEV.QUEUE.1",
                connection=CancelledConnection(),
            ),
            body=b"hello",
        )

        with pytest.raises(asyncio.CancelledError):
            await message.nack()

        assert message.committed is None
        assert message.raw_message.settled is None
