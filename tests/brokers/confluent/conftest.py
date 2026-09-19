from typing import Any

import pytest

from faststream.confluent import KafkaRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Any:
    return Settings()


@pytest.fixture()
def router() -> Any:
    return KafkaRouter()
