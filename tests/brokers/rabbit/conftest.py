from dataclasses import dataclass

import pytest

from faststream.rabbit import (
    RabbitExchange,
    RabbitRouter,
)


@dataclass
class Settings:
    url: str = "amqp://guest:guest@localhost:5672/"

    host: str = "localhost"
    port: int = 5672
    login: str = "guest"
    password: str = "guest"

    queue: str = "test_queue"


@pytest.fixture()
def exchange(queue):
    return RabbitExchange(name=queue)


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return RabbitRouter()
