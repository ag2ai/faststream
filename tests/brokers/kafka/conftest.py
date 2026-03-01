from dataclasses import dataclass

import pytest

from faststream.kafka import KafkaRouter


@dataclass
class Settings:
    url: str = "localhost:9092"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> KafkaRouter:
    return KafkaRouter()
