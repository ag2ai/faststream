from faststream import Config
from faststream._internal.constants import EMPTY


def test_key_is_stored() -> None:
    assert Config("IN_TOPIC").key == "IN_TOPIC"


def test_default_is_empty_when_not_given() -> None:
    assert Config("IN_TOPIC").default is EMPTY


def test_none_is_a_real_default() -> None:
    """`None` is a value, not "no default" — EMPTY is "no default"."""
    assert Config("STREAM", default=None).default is None


def test_repr_shows_the_key() -> None:
    assert repr(Config("IN_TOPIC")) == "Config('IN_TOPIC')"
    assert repr(Config("IN_TOPIC", default=None)) == "Config('IN_TOPIC', default=None)"
