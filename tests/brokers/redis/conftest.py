import pytest

from faststream.redis import RedisRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return RedisRouter()
