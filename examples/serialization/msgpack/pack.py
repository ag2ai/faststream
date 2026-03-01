import msgpack

from faststream import BaseMiddleware, FastStream, Logger
from faststream.rabbit import RabbitBroker, RabbitMessage


class MsgPackMiddleware(BaseMiddleware):
    async def publish_scope(self, call_next, msg, **options):
        return await call_next(
            msgpack.dumps(msg, use_bin_type=True),
            **options,
        )


def decode_message(msg: RabbitMessage):
    return msgpack.loads(msg.body)


broker = RabbitBroker(
    decoder=decode_message,
    middlewares=(MsgPackMiddleware,),
)
app = FastStream(broker)


@broker.subscriber("test")
async def consume(name: str, age: int, logger: Logger):
    logger.info("%s: %s", name, age)


@app.after_startup
async def publish():
    await broker.publish({"name": "John", "age": 25}, "test")
