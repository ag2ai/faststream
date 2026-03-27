from dataclasses import dataclass
from uuid import uuid4

import pytest

from faststream.mq.broker.router import MQRouter


@dataclass
class Settings:
    queue_manager: str = "QM1"
    channel: str = "DEV.APP.SVRCONN"
    host: str = "localhost"
    port: int = 1414
    username: str = "app"
    password: str = "password"
    reply_model_queue: str = "DEV.APP.MODEL.QUEUE"

    @property
    def conn_name(self) -> str:
        return f"{self.host}({self.port})"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> MQRouter:
    return MQRouter()


@pytest.fixture()
def queue() -> str:
    return f"DEV.Q{uuid4().hex[:20].upper()}"
