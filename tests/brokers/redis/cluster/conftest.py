from typing import Any

import pytest

from .settings import SettingsCluster


@pytest.fixture(scope="session")
def settings_cluster() -> Any:
    return SettingsCluster()
