from dataclasses import dataclass
from typing import Any

import pytest

from faststream.rabbit import (
    RabbitExchange,
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
def exchange(queue: str) -> Any:
    return RabbitExchange(name=queue)


@pytest.fixture(scope="session")
def settings() -> Any:
    return Settings()
