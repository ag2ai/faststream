from logging import Logger

from faststream import Context, apply_types
from faststream.rabbit import RabbitBroker


broker = RabbitBroker()


@broker.subscriber("test")
async def handler(body):
    nested_func(body)


@apply_types
def nested_func(body, logger: Logger = Context()):
    logger.info(body)
