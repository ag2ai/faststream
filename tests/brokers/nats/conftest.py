from dataclasses import dataclass

import pytest

from faststream.nats import JStream, NatsRouter


@dataclass
class Settings:
    url: str = "nats://localhost:4222"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def stream(queue: str) -> JStream:
    return JStream(queue)


@pytest.fixture()
def router() -> NatsRouter:
    return NatsRouter()
