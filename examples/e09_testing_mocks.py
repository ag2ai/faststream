import pytest

from faststream import FastStream
from faststream.rabbit import RabbitBroker, TestRabbitBroker

broker = RabbitBroker("amqp://guest:guest@localhost:5672/")
app = FastStream(broker)


publisher1 = broker.publisher("test-resp")
publisher2 = broker.publisher("test-resp2")


@publisher1
@publisher2
@broker.subscriber("test")
async def handle() -> str:
    return "response"


@pytest.mark.asyncio()
async def test_handle() -> None:
    async with TestRabbitBroker(broker) as br:
        await br.publish({"msg": "test"}, "test")

        # check an incoming message body
        handle.mock.assert_called_with({"msg": "test"})

        # check the publishers responses
        publisher1.mock.assert_called_once_with("response")
        publisher2.mock.assert_called_once_with("response")
