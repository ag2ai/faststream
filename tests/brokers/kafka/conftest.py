import pytest

from faststream.kafka import KafkaRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> KafkaRouter:
    return KafkaRouter()
