from dataclasses import dataclass

import pytest

from faststream.mqtt.broker.router import MQTTRouter


@dataclass
class Settings:
    host: str = "localhost"
    port: int = 1883


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(params=["3.1.1", "5.0"])
def mqtt_version(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture()
def router() -> MQTTRouter:
    return MQTTRouter()
