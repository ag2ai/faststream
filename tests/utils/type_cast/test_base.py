import pytest

from faststream._internal.utils import apply_types


@apply_types
def cast_int(t: int = 1) -> tuple[bool, int]:
    return isinstance(t, int), t


@apply_types
def cast_default(t: int = 1) -> tuple[bool, int]:
    return isinstance(t, int), t


def test_int() -> None:
    assert cast_int("1") == (True, 1)

    assert cast_int(t=1.0) == (True, 1)
    assert cast_int(2.0) == (True, 2)

    assert cast_int(t=True) == (True, 1)
    assert cast_int(False) == (True, 0)

    assert cast_int() == (True, 1)

    with pytest.raises(ValueError):  # noqa: PT011
        assert cast_int([])


def test_cast_default() -> None:
    assert cast_default("1") == (True, 1)

    assert cast_default(t=1.0) == (True, 1)
    assert cast_default(2.0) == (True, 2)

    assert cast_default(t=True) == (True, 1)
    assert cast_default(False) == (True, 0)

    assert cast_default() == (True, 1)

    with pytest.raises(ValueError):  # noqa: PT011
        assert cast_default([])
