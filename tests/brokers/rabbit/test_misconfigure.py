from typing import Any

import pytest

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.rabbit import RabbitBroker, RabbitRouter


@pytest.mark.rabbit()
def test_use_only_rabbit_router() -> None:
    broker = RabbitBroker()
    router: Any = NatsRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [RabbitRouter(), NatsRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)
