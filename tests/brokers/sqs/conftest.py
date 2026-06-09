from dataclasses import dataclass

import pytest

from faststream.sqs.broker.router import SQSRouter


@dataclass
class Settings:
    endpoint_url: str = "http://localhost:9324"
    region_name: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture()
def router() -> SQSRouter:
    return SQSRouter()
