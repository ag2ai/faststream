"""The contract every broker value object inherits: its name is an `Address`."""

from typing import TYPE_CHECKING

import pytest

from tests.marks import require_aiopika, require_nats, require_redis

if TYPE_CHECKING:
    from faststream._internal.proto import NameRequired


def rabbit_queue() -> type["NameRequired"]:
    from faststream.rabbit import RabbitQueue

    return RabbitQueue


def rabbit_exchange() -> type["NameRequired"]:
    from faststream.rabbit import RabbitExchange

    return RabbitExchange


def pub_sub() -> type["NameRequired"]:
    from faststream.redis import PubSub

    return PubSub


def list_sub() -> type["NameRequired"]:
    from faststream.redis import ListSub

    return ListSub


def stream_sub() -> type["NameRequired"]:
    from faststream.redis import StreamSub

    return StreamSub


def js_stream() -> type["NameRequired"]:
    from faststream.nats import JStream

    return JStream


def kv_watch() -> type["NameRequired"]:
    from faststream.nats import KvWatch

    return KvWatch


NAMED = (
    pytest.param(rabbit_queue, marks=require_aiopika, id="RabbitQueue"),
    pytest.param(rabbit_exchange, marks=require_aiopika, id="RabbitExchange"),
    pytest.param(pub_sub, marks=require_redis, id="PubSub"),
    pytest.param(list_sub, marks=require_redis, id="ListSub"),
    pytest.param(stream_sub, marks=require_redis, id="StreamSub"),
    pytest.param(js_stream, marks=require_nats, id="JStream"),
    pytest.param(kv_watch, marks=require_nats, id="KvWatch"),
)

PREFIXED = (NAMED[0], NAMED[2], NAMED[3], NAMED[4])
"""The four a Router prefix reaches, and the only four that expose `add_prefix`."""

UNPREFIXED = (NAMED[1], NAMED[5], NAMED[6])
"""An exchange, a stream and a bucket are named outside the Router's namespace."""

VERBATIM = tuple(p for p in NAMED if p.id != "PubSub")
"""Every name but a channel reaches its broker as the characters it was written with."""


@pytest.mark.parametrize("type_", NAMED)
def test_a_name_is_an_address(type_) -> None:
    obj = type_()("logs")

    assert obj.address.template == "logs"
    assert obj.name == "logs"


@pytest.mark.parametrize("type_", NAMED)
def test_an_address_is_assigned_nowhere_outside_construction(type_) -> None:
    """A writeable address would make ADR-0005's covariant parameter unsound.

    mypy permits covariance over a writeable attribute on a non-protocol class,
    so nothing in the build would report it. This is what reports it.
    """
    obj = type_()("logs")

    with pytest.raises(AttributeError):
        obj.name = "reassigned"

    with pytest.raises(AttributeError):
        obj.address = obj.address

    assert obj.name == "logs"


@pytest.mark.parametrize("type_", PREFIXED)
def test_a_prefix_decorates_the_name_through_its_address(type_) -> None:
    obj = type_()("logs")
    prefixed = obj.add_prefix("prefix_")

    assert prefixed.name == "prefix_logs"
    assert prefixed.address.template == "prefix_logs"
    assert obj.name == "logs", "the original is left alone"


@pytest.mark.parametrize("type_", UNPREFIXED)
def test_a_value_object_outside_the_router_namespace_has_no_prefix_pass(type_) -> None:
    """ADR-0006 has the object-level pass going away, not spreading.

    An exchange is never decorated — `rabbit/address.py` says so and does not
    call one — so exposing `add_prefix` on it would make the wrong call
    type-check.
    """
    assert not hasattr(type_(), "add_prefix")


@pytest.mark.parametrize("type_", VERBATIM)
def test_a_brace_in_a_name_is_a_character_like_any_other(type_) -> None:
    assert type_()("logs.{level}").name == "logs.{level}"


@require_redis
def test_a_channel_is_a_template_and_its_prefix_follows_it() -> None:
    from faststream.redis import PubSub

    channel = PubSub("logs.{level}").add_prefix("prefix_")

    assert channel.address.template == "prefix_logs.{level}"
    assert channel.name == "prefix_logs.*"
    assert channel.path_regex is not None


@require_aiopika
def test_a_queue_prefixes_its_name_and_its_routing_key_together() -> None:
    from faststream.rabbit import RabbitQueue

    queue = RabbitQueue("logs", routing_key="logs.{level}").add_prefix("prefix_")

    assert queue.name == "prefix_logs"
    assert queue.routing_key == "prefix_logs.*"
    assert queue.routing_template() == "prefix_logs.{level}"


@require_aiopika
def test_a_queue_with_no_binding_still_binds_by_its_prefixed_name() -> None:
    """An empty routing key is not one the prefix has anything to decorate."""
    from faststream.rabbit import RabbitQueue

    queue = RabbitQueue("logs").add_prefix("prefix_")

    assert queue.routing() == "prefix_logs"
