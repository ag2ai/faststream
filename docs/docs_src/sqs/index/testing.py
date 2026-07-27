import pytest

from faststream.sqs import SQSBroker, TestSQSBroker

broker = SQSBroker()


@broker.subscriber("test-queue")
async def handler(msg: str) -> str:
    return msg + "!"


@pytest.mark.asyncio
async def test_handler() -> None:
    async with TestSQSBroker(broker) as br:
        await br.publish("hello", "test-queue")
        handler.mock.assert_called_once_with("hello")
