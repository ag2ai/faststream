from typing import Any

import pytest

from faststream.specification import ExternalDocs
from faststream.specification.asyncapi.v2_6_0.schema import ExternalDocs as AsyncAPIDocs


@pytest.mark.parametrize(
    ("arg", "result"),
    (
        pytest.param(
            None,
            None,
            id="None",
        ),
        pytest.param(
            ExternalDocs(description="test", url="http://docs.com"),
            AsyncAPIDocs(description="test", url="http://docs.com"),
            id="ExternalDocs object",
        ),
        pytest.param(
            {"description": "test", "url": "http://docs.com"},
            AsyncAPIDocs(description="test", url="http://docs.com"),
            id="ExternalDocs dict",
        ),
        pytest.param(
            {"description": "test", "url": "http://docs.com", "extra": "test"},
            {"description": "test", "url": "http://docs.com", "extra": "test"},
            id="Unknown dict",
        ),
    ),
)
def test_external_docs_factory_method(arg: Any, result: Any) -> None:
    assert AsyncAPIDocs.from_spec(arg) == result
