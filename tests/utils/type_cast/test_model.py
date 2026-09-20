from typing import Any

import pytest
from pydantic import BaseModel

from faststream._internal.utils import apply_types


class Base(BaseModel):
    field: int


@apply_types
def cast_model(t: Base) -> tuple[bool, Base]:
    return isinstance(t, Base), t


@pytest.mark.parametrize(
    "value",
    (
        pytest.param({"field": 1}, id="dict"),
        pytest.param(Base(field=1), id="model"),
        pytest.param({"field": "1"}, id="dict with str"),
    ),
)
def test_model(value: Any) -> None:
    assert cast_model(value) == (True, Base(field=1))


def test_model_wrong_value() -> None:
    value: Any = ("field", 1)

    with pytest.raises(ValueError):  # noqa: PT011
        cast_model(value)
