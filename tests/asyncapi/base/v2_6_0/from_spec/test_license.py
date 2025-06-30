from typing import Any

import pytest

from faststream.specification import License
from faststream.specification.asyncapi.v2_6_0.schema import License as AsyncAPICLicense


@pytest.mark.parametrize(
    ("arg", "result"),
    (
        pytest.param(
            None,
            None,
            id="None",
        ),
        pytest.param(
            License(name="test", url="http://license.com"),
            AsyncAPICLicense(name="test", url="http://license.com"),
            id="License object",
        ),
        pytest.param(
            {"name": "test", "url": "http://license.com"},
            AsyncAPICLicense(name="test", url="http://license.com"),
            id="License dict",
        ),
        pytest.param(
            {"name": "test", "url": "http://license.com", "extra": "test"},
            {"name": "test", "url": "http://license.com", "extra": "test"},
            id="Unknown dict",
        ),
    ),
)
def test_license_factory_method(arg: Any, result: Any) -> None:
    assert AsyncAPICLicense.from_spec(arg) == result
