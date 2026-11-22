import pytest

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()


@pytest.fixture(params=["3.1.1", "5.0"])
def mqtt_version(request: pytest.FixtureRequest) -> str:
    version: str = request.param
    return version
