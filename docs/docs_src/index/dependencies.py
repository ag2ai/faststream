from faststream import Depends, Logger
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

async def base_dep(user_id: int) -> bool:
    return True

@broker.subscriber("in-test")
async def base_handler(
    user: str,
    logger: Logger,
    dep: bool = Depends(base_dep),
):
    assert dep is True
    logger.info(user)
