import copy
import pickle
from collections.abc import Callable
from typing import cast

import pytest

from faststream._internal.kafka.tombstone import TOMBSTONE, Tombstone


def test_a_tombstone_carries_no_data() -> None:
    with pytest.raises(ValueError, match="carries no data"):
        Tombstone(b"data")


def test_a_tombstone_is_an_empty_bytes() -> None:
    assert TOMBSTONE == b""
    assert len(TOMBSTONE) == 0
    assert not TOMBSTONE
    assert isinstance(TOMBSTONE, bytes)


def test_a_tombstone_says_what_it_is_however_it_is_printed() -> None:
    assert repr(TOMBSTONE) == "TOMBSTONE"
    assert str(TOMBSTONE) == "TOMBSTONE"
    assert f"{TOMBSTONE}" == "TOMBSTONE"
    assert "%s" % cast("object", TOMBSTONE) == "TOMBSTONE"  # ruff: ignore[printf-string-formatting]


@pytest.mark.parametrize(
    "clone",
    (
        pytest.param(lambda value: pickle.loads(pickle.dumps(value)), id="pickle"),
        pytest.param(copy.copy, id="copy"),
        pytest.param(copy.deepcopy, id="deepcopy"),
    ),
)
def test_a_tombstone_survives_a_round_trip(clone: Callable[[Tombstone], object]) -> None:
    restored = clone(TOMBSTONE)

    assert isinstance(restored, Tombstone)
    assert restored == b""
