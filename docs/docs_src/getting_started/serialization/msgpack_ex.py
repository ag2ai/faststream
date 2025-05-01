import msgpack

from faststream import FastStream, Logger
from faststream.rabbit import RabbitBroker, RabbitMessage

broker = RabbitBroker()
app = FastStream(broker)


async def decode_message(msg: RabbitMessage):
    return msgpack.loads(msg.body)


@broker.subscriber("test", decoder=decode_message)
async def consume(name: str, age: int, logger: Logger):
    logger.info("%s: %s", name, age)


@app.after_startup
async def publish():
    body = msgpack.dumps({"name": "John", "age": 25}, use_bin_type=True)
    await broker.publish(body, "test")
