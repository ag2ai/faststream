"""The contract every broker value object inherits: its name is an `Address`."""

import pytest

from faststream._internal.proto import NameRequired
from faststream.nats import JStream, KvWatch
from faststream.rabbit import RabbitExchange, RabbitQueue
from faststream.redis import ListSub, PubSub, StreamSub

NAMED_TYPES = (RabbitQueue, RabbitExchange, PubSub, ListSub, StreamSub, JStream, KvWatch)

PREFIXED_TYPES = (RabbitQueue, PubSub, ListSub, StreamSub)
"""The four a Router prefix actually reaches.

An exchange is never decorated — it lives outside the Router's namespace — and
neither a stream nor a bucket is read through a prefixing call. They inherit the
pass all the same, so what is pinned here is only what is composed, not who calls it.
"""

VERBATIM_TYPES = tuple(t for t in NAMED_TYPES if t is not PubSub)
"""Every name but a channel reaches its broker as the characters it was written with."""


def named(type_: type[NameRequired]) -> NameRequired:
    return type_("logs")


@pytest.mark.parametrize("type_", NAMED_TYPES, ids=lambda t: t.__name__)
def test_a_name_is_an_address(type_: type[NameRequired]) -> None:
    obj = named(type_)

    assert obj.address.template == "logs"
    assert obj.name == "logs"


@pytest.mark.parametrize("type_", PREFIXED_TYPES, ids=lambda t: t.__name__)
def test_a_prefix_decorates_the_name_through_its_address(
    type_: type[NameRequired],
) -> None:
    obj = named(type_)
    prefixed = obj.add_prefix("prefix_")

    assert prefixed.name == "prefix_logs"
    assert prefixed.address.template == "prefix_logs"
    assert obj.name == "logs", "the original is left alone"


@pytest.mark.parametrize("type_", VERBATIM_TYPES, ids=lambda t: t.__name__)
def test_a_brace_in_a_name_is_a_character_like_any_other(
    type_: type[NameRequired],
) -> None:
    braced = type_("logs.{level}")

    assert braced.name == "logs.{level}"
    assert braced.add_prefix("prefix_").name == "prefix_logs.{level}"


@pytest.mark.parametrize("type_", NAMED_TYPES, ids=lambda t: t.__name__)
def test_a_name_is_assigned_nowhere_outside_construction(
    type_: type[NameRequired],
) -> None:
    """A writeable name would make ticket 17's covariant parameter unsound.

    mypy permits covariance over a writeable attribute on a non-protocol class,
    so nothing in the build would report it. This is what reports it.
    """
    obj = named(type_)

    with pytest.raises(AttributeError):
        obj.name = "reassigned"  # type: ignore[misc]

    assert obj.name == "logs"


def test_a_channel_is_a_template_and_its_prefix_follows_it() -> None:
    channel = PubSub("logs.{level}").add_prefix("prefix_")

    assert channel.address.template == "prefix_logs.{level}"
    assert channel.name == "prefix_logs.*"
    assert channel.path_regex is not None


def test_a_queue_prefixes_its_name_and_its_routing_key_together() -> None:
    queue = RabbitQueue("logs", routing_key="logs.{level}").add_prefix("prefix_")

    assert queue.name == "prefix_logs"
    assert queue.routing_key == "prefix_logs.*"
    assert queue.routing_template() == "prefix_logs.{level}"


def test_a_stream_keeps_its_declaration_config_in_step_with_its_name() -> None:
    """Not that anything prefixes a stream, but that its `StreamConfig` follows."""
    stream = JStream("logs").add_prefix("prefix_")

    assert stream.name == "prefix_logs"
    assert stream.config.name == "prefix_logs"
