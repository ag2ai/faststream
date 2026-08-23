from collections.abc import Iterator
from typing import Any
from urllib.parse import unquote

from faststream._internal.broker import BrokerUsecase


def iter_strings(node: Any) -> Iterator[str]:
    """Every string a generated schema contains, keys and values alike."""
    if isinstance(node, str):
        yield node

    elif isinstance(node, dict):
        for key, value in node.items():
            yield key
            yield from iter_strings(value)

    elif isinstance(node, list):
        for item in node:
            yield from iter_strings(item)


class AddressTemplateTestcase:
    """The schema shows the Address template, never the Broker address.

    `logs.{level}` is what the developer wrote and what documents the contract;
    `logs.*` is one broker's way of asking for that family of addresses, and the
    same declaration compiles to a different string on every broker. So no
    generated document may contain it — not in a channel name, not in a binding.

    Mix into a version factory (`AsyncAPI260Factory`, `AsyncAPI300Factory`), which
    supplies the `get_spec` these tests call.

    Confluent is absent on purpose: it has no template support, so its declared
    address and its Broker address are always the same string and there is nothing
    for this testcase to tell apart.
    """

    broker_class: type[BrokerUsecase[Any, Any]]

    address_template = "logs.{level}"
    """An Address template to declare an endpoint with."""

    broker_address = "logs.*"
    """The Broker address that template stands for — the string no document may
    contain. A broker that never compiles its addresses has none of its own to
    leak; the assertion then guards against it gaining one."""

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(self.address_template)

    def declare_publisher(self, broker: Any) -> None:
        broker.publisher(self.address_template)

    def test_subscriber_renders_the_template(self) -> None:
        broker = self.broker_class()
        self.declare_subscriber(broker)
        self.assert_renders_the_template(broker)

    def test_publisher_renders_the_template(self) -> None:
        broker = self.broker_class()
        self.declare_publisher(broker)
        self.assert_renders_the_template(broker)

    def assert_renders_the_template(self, broker: Any) -> None:
        schema = self.get_spec(broker).to_jsonable()

        # Where an address is named: `channels` for most brokers, `operations` for
        # RabbitMQ's routing key. Deliberately not `components` — a payload title
        # carries the address too, and would satisfy this without a single channel
        # or binding being right.
        addressed = [schema["channels"], schema.get("operations", {})]

        assert any(self.address_template in s for s in iter_strings(addressed)), (
            f"{self.address_template!r} names no channel or binding in {addressed}"
        )

        # The leak, in contrast, is checked over the whole document: a wildcard has
        # no business in a payload title either. 3.0 percent-encodes the address
        # into its `$ref`s (`logs.%2A:Publisher`), so each string is checked decoded
        # as well — otherwise an address surviving only in a component key passes.
        leaked = [
            s
            for s in iter_strings(schema)
            if self.broker_address in s or self.broker_address in unquote(s)
        ]
        assert not leaked, f"{self.broker_address!r} leaked into {leaked}"
