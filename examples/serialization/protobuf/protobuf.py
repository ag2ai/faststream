from message_pb2 import Person

from faststream import FastStream, Logger, NoCast
from faststream.rabbit import RabbitBroker, RabbitMessage

broker = RabbitBroker()
app = FastStream(broker)


async def decode_message(msg: RabbitMessage) -> Person:
    decoded = Person()
    decoded.ParseFromString(msg.body)
    return decoded


@broker.subscriber("test", decoder=decode_message)
async def consume(body: NoCast[Person], logger: Logger):
    logger.info(body)


@app.after_startup
async def publish():
    body = Person(name="john", age=25).SerializeToString()
    await broker.publish(body, "test")
