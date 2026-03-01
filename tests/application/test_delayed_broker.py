import pytest

from faststream._internal.application import StartAbleApplication
from faststream.exceptions import SetupError
from faststream.rabbit import RabbitBroker


def test_set_broker() -> None:
    app = StartAbleApplication()

    assert app.broker is None

    broker = RabbitBroker()
    app.set_broker(broker)

    assert app.broker is broker


def test_set_more_than_once_broker() -> None:
    app = StartAbleApplication()
    broker_1 = RabbitBroker()
    broker_2 = RabbitBroker()

    app.set_broker(broker_1)

    with pytest.raises(
        SetupError,
        match=f"`{app}` already has a broker. You can't use multiple brokers until 1.0.0 release.",
    ):
        app.set_broker(broker_2)


@pytest.mark.asyncio()
async def test_start_not_setup_broker() -> None:
    app = StartAbleApplication()

    with pytest.raises(AssertionError, match="You should setup a broker"):
        await app._start_broker()


@pytest.mark.asyncio()
async def test_di_reconfigured() -> None:
    broker = RabbitBroker()
    app = StartAbleApplication()
    app.set_broker(broker)

    assert broker.context.get("app") is app
