from typing import Any

from faststream._internal.broker import BrokerUsecase


class AddressFieldTestcase:
    """The channel key names the channel; the address names the address.

    A subscriber's channel is keyed `<name>:<handler>`, so the key is never the
    address on its own — `test:Handle` is a name for a channel, not a string
    anybody publishes to. 3.0 has an `address` field to answer the second question
    and 2.6 has none, which is why the two versions assert different things and
    share only the declarations below.

    Mix into a version factory (`AsyncAPI260Factory`, `AsyncAPI300Factory`), which
    supplies the `get_spec` these tests call.
    """

    broker_class: type[BrokerUsecase[Any, Any]]

    address = "test"
    """The address to declare an endpoint with."""

    rendered_address = "test"
    """What 3.0's `address` must say for that declaration.

    The same string, except where a broker's declaration carries more than the
    address: MQTT's `$share/<group>/` names a shared subscription, and RabbitMQ's
    queue is a place to read from rather than a string to address.
    """

    def declare_subscriber(self, broker: Any) -> None:
        broker.subscriber(self.address)

    def declare_publisher(self, broker: Any) -> None:
        broker.publisher(self.address)

    def only_channel(self, broker: Any) -> tuple[str, dict[str, Any]]:
        """The single channel one declaration produced, keyed as the document keys it."""
        channels = self.get_spec(broker).to_jsonable()["channels"]
        assert len(channels) == 1, channels
        return next(iter(channels.items()))

    def subscriber_channel(self) -> tuple[str, dict[str, Any]]:
        broker = self.broker_class()
        self.declare_subscriber(broker)
        return self.only_channel(broker)

    def publisher_channel(self) -> tuple[str, dict[str, Any]]:
        broker = self.broker_class()
        self.declare_publisher(broker)
        return self.only_channel(broker)
