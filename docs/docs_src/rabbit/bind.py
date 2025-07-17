import aio_pika
from faststream import FastStream
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

broker = RabbitBroker()
app = FastStream(broker)


some_queue = RabbitQueue(
    name="some-queue",
    durable=True,
)

some_exchange = RabbitExchange(
    name="some-exchange",
    type=ExchangeType.FANOUT,
)

@app.after_startup
async def bind_queue_exchange():
    queue: aio_pika.RobustQueue = await broker.declare_queue(
        some_queue,
    )

    exchange: aio_pika.RobustExchange = await broker.declare_exchange(
        some_exchange,
    )

    await queue.bind(
        exchange=exchange,
        routing_key=queue.name,  # Optional parameter
    )
