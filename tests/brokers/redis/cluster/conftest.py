import pytest

from .settings import SettingsCluster


@pytest.fixture(scope="session")
def settings_cluster():
    return SettingsCluster()
