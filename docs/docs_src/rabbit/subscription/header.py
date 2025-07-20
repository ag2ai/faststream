from faststream import FastStream, Logger
from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

broker = RabbitBroker()
app = FastStream(broker)

exch = RabbitExchange("exchange", auto_delete=True, type=ExchangeType.HEADERS)

queue_1 = RabbitQueue(
    "test-queue-1",
    auto_delete=True,
    bind_arguments={"key": 1},
)
queue_2 = RabbitQueue(
    "test-queue-2",
    auto_delete=True,
    bind_arguments={"key": 2, "key2": 2, "x-match": "any"},
)
queue_3 = RabbitQueue(
    "test-queue-3",
    auto_delete=True,
    bind_arguments={"key": 2, "key2": 2, "x-match": "all"},
)


@broker.subscriber(queue_1, exch)
async def base_handler1(logger: Logger):
    logger.info("base_handler1")


@broker.subscriber(queue_1, exch)  # another service
async def base_handler2(logger: Logger):
    logger.info("base_handler2")


@broker.subscriber(queue_2, exch)
async def base_handler3(logger: Logger):
    logger.info("base_handler3")


@broker.subscriber(queue_3, exch)
async def base_handler4(logger: Logger):
    logger.info("base_handler4")


@app.after_startup
async def send_messages():
    await broker.publish(exchange=exch, headers={"key": 1})  # handlers: 1
    await broker.publish(exchange=exch, headers={"key": 1})  # handlers: 2
    await broker.publish(exchange=exch, headers={"key": 1})  # handlers: 1
    await broker.publish(exchange=exch, headers={"key": 2})  # handlers: 3
    await broker.publish(exchange=exch, headers={"key2": 2})  # handlers: 3
    await broker.publish(
        exchange=exch, headers={"key": 2, "key2": 2.0},
    )  # handlers: 3, 4
