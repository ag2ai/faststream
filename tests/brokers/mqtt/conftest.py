from dataclasses import dataclass

import pytest


@dataclass
class Settings: ...


@pytest.fixture(scope="session")
def settings() -> Settings:
    return Settings()
