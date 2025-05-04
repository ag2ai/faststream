from dataclasses import dataclass

import pytest

from faststream.rabbit import (
    RabbitExchange,
    RabbitRouter,
)


@dataclass
class Settings:
    url = "amqp://guest:guest@localhost:5672/"  # pragma: allowlist secret

    host = "localhost"
    port = 5672
    login = "guest"
    password = "guest"  # pragma: allowlist secret

    queue = "test_queue"


@pytest.fixture()
def exchange(queue):
    return RabbitExchange(name=queue)


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return RabbitRouter()
