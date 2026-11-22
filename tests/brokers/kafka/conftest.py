import pytest

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()
