import pytest

from faststream.confluent import KafkaRouter

from .settings import Settings


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return KafkaRouter()
