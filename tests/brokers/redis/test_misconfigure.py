import pytest

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.redis import RedisBroker, RedisRouter


@pytest.mark.redis()
def test_use_only_redis_router() -> None:
    broker = RedisBroker()
    router = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers = [RedisRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(routers)
