from typing import Awaitable, Callable

from aiokafka import ConsumerRecord

from faststream import FastStream
from faststream.kafka import KafkaBroker, KafkaMessage


async def custom_parser(
    msg: ConsumerRecord,
    original_parser: Callable[[ConsumerRecord], Awaitable[KafkaMessage]],
) -> KafkaMessage:
    parsed_msg = await original_parser(msg)
    parsed_msg.message_id = parsed_msg.headers.get("custom_message_id")
    return parsed_msg


broker = KafkaBroker(parser=custom_parser)
app = FastStream(broker)


@broker.subscriber("test")
async def handle():
    ...


@app.after_startup
async def test():
    await broker.publish("", "test", headers={"custom_message_id": "1"})
