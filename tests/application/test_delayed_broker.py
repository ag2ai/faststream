import pytest

from faststream._internal.application import StartAbleApplication
from faststream.rabbit import RabbitBroker


def test_set_broker() -> None:
    app = StartAbleApplication()

    assert app.broker is None

    broker = RabbitBroker()
    app.add_broker(broker)

    assert app.broker is broker


def test_set_more_than_once_broker() -> None:
    app = StartAbleApplication()
    broker_1 = RabbitBroker()
    broker_2 = RabbitBroker()

    app.add_broker(broker_1)
    app.add_broker(broker_2)


@pytest.mark.asyncio()
async def test_start_not_setup_broker() -> None:
    app = StartAbleApplication()

    with pytest.raises(AssertionError, match="You should setup a broker"):
        await app._start_broker()


@pytest.mark.asyncio()
async def test_di_reconfigured() -> None:
    broker = RabbitBroker()
    app = StartAbleApplication()
    app.add_broker(broker)
    assert broker.context.get("app") is app
