from typing import Any

import pytest

from faststream.specification import ExternalDocs, Tag
from faststream.specification.asyncapi.v2_6_0.schema import (
    ExternalDocs as AsyncAPIDocs,
    Tag as AsyncAPITag,
)


@pytest.mark.parametrize(
    ("arg", "result"),
    (
        pytest.param(
            Tag(
                name="test",
                description="test",
                external_docs=ExternalDocs(url="http://docs.com"),
            ),
            AsyncAPITag(
                name="test",
                description="test",
                externalDocs=AsyncAPIDocs(url="http://docs.com"),
            ),
            id="Tag object",
        ),
        pytest.param(
            {
                "name": "test",
                "description": "test",
                "external_docs": {"url": "http://docs.com"},
            },
            AsyncAPITag(
                name="test",
                description="test",
                externalDocs=AsyncAPIDocs(url="http://docs.com"),
            ),
            id="Tag dict",
        ),
        pytest.param(
            {"name": "test", "description": "test", "extra": "test"},
            {"name": "test", "description": "test", "extra": "test"},
            id="Unknown dict",
        ),
    ),
)
def test_tag_factory_method(arg: Any, result: Any) -> None:
    assert AsyncAPITag.from_spec(arg) == result
