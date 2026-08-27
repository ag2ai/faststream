from typing import Any

from faststream._internal.broker import BrokerUsecase


class TopicChannelsTestcase:
    """One subscriber over several topics is one channel per topic, in order.

    Mixed into Kafka and Confluent only: they are the brokers whose subscriber takes
    more than one address at a declaration site.
    """

    broker_class: type[BrokerUsecase[Any, Any]]

    def test_channels_follow_declaration_order(self) -> None:
        broker = self.broker_class()

        @broker.subscriber("gamma", "alpha", "beta")
        async def handle() -> None: ...

        # a document that shuffles its own channels cannot be diffed in CI
        assert list(self.get_spec(broker).to_jsonable()["channels"]) == [
            "gamma:Handle",
            "alpha:Handle",
            "beta:Handle",
        ]
