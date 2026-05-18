from dataclasses import dataclass, field

import pytest

from faststream.redis import RedisRouter


@dataclass
class Settings:
    url: str = "redis://localhost:6379"
    host: str = "localhost"
    port: int = 6379


@dataclass
class SettingsCluster:
    url: str = "redis://127.0.0.1:7001"
    host: str = "127.0.0.1"
    port: int = 7001
    startup_nodes: list[tuple[str, int]] = field(
        default_factory=lambda: [("127.0.0.1", 7002), ("127.0.0.1", 7003)],
    )


@pytest.fixture(scope="session")
def settings():
    return Settings()


@pytest.fixture(scope="session")
def settings_cluster():
    return SettingsCluster()


@pytest.fixture()
def router():
    return RedisRouter()
