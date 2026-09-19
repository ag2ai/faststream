from typing import Any

import pytest

from faststream.redis import RedisRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Any:
    return Settings()


@pytest.fixture()
def router() -> Any:
    return RedisRouter()
