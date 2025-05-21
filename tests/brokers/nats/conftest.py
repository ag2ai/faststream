from dataclasses import dataclass

import pytest

from faststream.nats import JStream, NatsRouter


@dataclass
class Settings:
    url = "nats://localhost:4222"  # pragma: allowlist secret


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def stream(queue):
    return JStream(queue)


@pytest.fixture()
def router():
    return NatsRouter()
