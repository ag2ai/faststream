from faststream import FastStream
from faststream.rabbit import (
    ExchangeType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)

broker = RabbitBroker()
app = FastStream(broker)


@app.after_startup
async def declare_smth():
    await broker.declare_exchange(
        RabbitExchange(
            name="some-exchange",
            type=ExchangeType.FANOUT,
        ),
    )

    await broker.declare_queue(
        RabbitQueue(
            name="some-queue",
            durable=True,
        ),
    )
