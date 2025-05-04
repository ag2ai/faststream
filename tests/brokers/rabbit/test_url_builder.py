from typing import Any

import pytest
from yarl import URL

from faststream.rabbit.utils import build_url


@pytest.mark.parametrize(
    ("url_kwargs", "expected_url"),
    (
        pytest.param(
            {},
            URL("amqp://guest:guest@localhost:5672/"),  # pragma: allowlist secret
            id="blank params use defaults",
        ),
        pytest.param(
            {"ssl": True},
            URL("amqps://guest:guest@localhost:5672/"),  # pragma: allowlist secret
            id="ssl affects protocol",
        ),
        pytest.param(
            {"url": "fake", "virtualhost": "/", "host": "host"},
            URL("amqp://guest:guest@host:5672/"),  # pragma: allowlist secret
            id="kwargs overrides url",
        ),
        pytest.param(
            {"virtualhost": "//test"},  # pragma: allowlist secret
            URL(
                "amqp://guest:guest@localhost:5672//test"  # pragma: allowlist secret
            ),
            id="exotic virtualhost",
        ),
    ),
)
def test_unpack_args(url_kwargs: dict[str, Any], expected_url: URL) -> None:
    url = build_url(**url_kwargs)
    assert url == expected_url
