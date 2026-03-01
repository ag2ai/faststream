import pytest
from pydantic import BaseModel

from faststream._internal.utils import apply_types


class Base(BaseModel):
    field: int


@apply_types
def cast_model(t: Base) -> tuple[bool, Base]:
    return isinstance(t, Base), t


def test_model() -> None:
    is_casted, m = cast_model({"field": 1})
    assert is_casted, m.field == (True, 1)

    is_casted, m = cast_model(Base(field=1))
    assert is_casted, m.field == (True, 1)

    is_casted, m = cast_model({"field": "1"})
    assert is_casted, m.field == (True, 1)

    with pytest.raises(ValueError):  # noqa: PT011
        cast_model(("field", 1))
