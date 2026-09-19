from typing import Any

import pytest

from .settings import Settings


@pytest.fixture(scope="session")
def settings() -> Any:
    return Settings()
