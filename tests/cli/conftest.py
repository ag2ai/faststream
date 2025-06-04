import pytest

from faststream import FastStream


@pytest.fixture()
def broker():
    # separate import from e2e tests
    from faststream.rabbit import RabbitBroker

    return RabbitBroker()


@pytest.fixture()
def app_without_logger(broker) -> FastStream:
    return FastStream(broker, logger=None)


@pytest.fixture()
def app(broker) -> FastStream:
    return FastStream(broker)


@pytest.fixture()
def app_without_broker() -> FastStream:
    return FastStream()
