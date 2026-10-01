from typing import Any

import pytest

from faststream.exceptions import SetupError
from faststream.nats import NatsRouter
from faststream.nats.broker.broker import NatsBroker
from faststream.rabbit import RabbitRouter


@pytest.mark.nats()
def test_use_only_nats_router() -> None:
    broker = NatsBroker()
    router: Any = RabbitRouter()

    with pytest.raises(SetupError):
        broker.include_router(router)

    routers: list[Any] = [NatsRouter(), RabbitRouter()]

    with pytest.raises(SetupError):
        broker.include_routers(*routers)
