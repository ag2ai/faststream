from dataclasses import dataclass

import pytest

from faststream.confluent import KafkaRouter


@dataclass
class Settings:
    """A class to represent the settings for the Kafka broker."""

    url = "localhost:9092"


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture()
def router():
    return KafkaRouter()
