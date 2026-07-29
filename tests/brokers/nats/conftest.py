import pytest

from faststream.nats import JStream, NatsRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def stream(queue: str) -> JStream:
    return JStream(queue)


@pytest.fixture()
def router() -> NatsRouter:
    return NatsRouter()
