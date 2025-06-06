from dataclasses import dataclass

import pytest

from faststream.redis import RedisRouter


@dataclass
class Settings:
    url: str = "redis://localhost:6379"  # pragma: allowlist secret
    host: str = "localhost"
    port: int = 6379


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return RedisRouter()
