import pytest

from faststream._internal.application import StartAbleApplication
from faststream._internal.context import ContextRepo
from faststream._internal.di import FastDependsConfig
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


def test_broker_and_app_contexts_merge() -> None:
    broker = RabbitBroker(
        context=ContextRepo({
            "broker_dependency": 1,
            "override_dependency": 2,
        })
    )

    config = FastDependsConfig(
        context=ContextRepo({
            "application_dependency": 3,
            "override_dependency": 4,
        })
    )
    app = StartAbleApplication(config=config)
    application_context = app.context

    # if the broker binds to the application,
    # the broker modifies the application context and uses it as its own.
    app.set_broker(broker)

    assert app.context is application_context
    assert broker.context is application_context
    assert app.context.get("broker_dependency") == 1
    assert app.context.get("application_dependency") == 3
    # the broker context overwrites the application context
    assert app.context.get("override_dependency") == 2
