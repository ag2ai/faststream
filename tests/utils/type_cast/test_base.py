from typing import Any

import pytest

from faststream._internal.utils import apply_types


@apply_types
def cast_int(t: int = 1) -> tuple[bool, int]:
    return isinstance(t, int), t


@apply_types
def cast_default(t: int = 1) -> tuple[bool, int]:
    return isinstance(t, int), t


CASTED_VALUES = (
    pytest.param("1", 1, id="str"),
    pytest.param(1.0, 1, id="float"),
    pytest.param(2.0, 2, id="another float"),
    pytest.param(True, 1, id="true"),
    pytest.param(False, 0, id="false"),
)


@pytest.mark.parametrize(("value", "expected"), CASTED_VALUES)
def test_int(value: Any, expected: int) -> None:
    assert cast_int(value) == (True, expected)
    assert cast_int(t=value) == (True, expected)


def test_int_default() -> None:
    assert cast_int() == (True, 1)


def test_int_wrong_value() -> None:
    value: Any = []

    with pytest.raises(ValueError):  # noqa: PT011
        cast_int(value)


@pytest.mark.parametrize(("value", "expected"), CASTED_VALUES)
def test_cast_default(value: Any, expected: int) -> None:
    assert cast_default(value) == (True, expected)
    assert cast_default(t=value) == (True, expected)


def test_cast_default_default() -> None:
    assert cast_default() == (True, 1)


def test_cast_default_wrong_value() -> None:
    value: Any = []

    with pytest.raises(ValueError):  # noqa: PT011
        cast_default(value)
