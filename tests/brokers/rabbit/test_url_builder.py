from typing import Any

import pytest
from yarl import URL

from faststream.rabbit.utils import build_url


@pytest.mark.rabbit()
@pytest.mark.parametrize(
    ("url_kwargs", "expected_url"),
    (
        pytest.param(
            {},
            URL("amqp://guest:guest@localhost:5672/"),
            id="blank params use defaults",
        ),
        pytest.param(
            {"ssl": True},
            URL("amqps://guest:guest@localhost:5672/"),
            id="ssl affects protocol",
        ),
        pytest.param(
            {"url": "fake", "virtualhost": "/", "host": "host"},
            URL("amqp://guest:guest@host:5672/"),
            id="kwargs overrides url",
        ),
        pytest.param(
            {"virtualhost": "//test"},
            URL("amqp://guest:guest@localhost:5672//test"),
            id="exotic virtualhost",
        ),
        pytest.param(
            {"virtualhost": "/test"},
            URL("amqp://guest:guest@localhost:5672//test"),
            id="virtualhost with leading slash",
        ),
        pytest.param(
            {"url": "amqp://guest:guest@localhost:5672", "virtualhost": "/test"},
            URL("amqp://guest:guest@localhost:5672//test"),
            id="url combined with slashed virtualhost",
        ),
        pytest.param(
            {"url": "amqp://guest:guest@localhost:5672", "virtualhost": "test"},
            URL("amqp://guest:guest@localhost:5672/test"),
            id="url no slash virtualhost without slash",
        ),
        pytest.param(
            {"virtualhost": "test"},
            URL("amqp://guest:guest@localhost:5672/test"),
            id="url with slash virtualhost without slash",
        ),
    ),
)
def test_unpack_args(url_kwargs: dict[str, Any], expected_url: URL) -> None:
    url = build_url(**url_kwargs)
    assert url == expected_url
