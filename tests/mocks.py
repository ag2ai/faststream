from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any
from unittest.mock import Mock

from pytest import MonkeyPatch  # noqa: PT013


@contextmanager
def mock_pydantic_settings_env(env_mapping: Mapping[str, Any]) -> None:
    with MonkeyPatch().context() as c:
        mock = Mock()
        mock.return_value = env_mapping
        c.setattr(
            "pydantic_settings.sources.DotEnvSettingsSource._read_env_files",
            mock,
        )
        yield
