from faststream import Context, apply_types

from faststream.rabbit import RabbitBroker


broker = RabbitBroker()


@broker.subscriber("test")
async def handler(body):
    nested_func(body)


@apply_types(context__=broker.context)
def nested_func(body, logger=Context()):
    logger.info(body)
