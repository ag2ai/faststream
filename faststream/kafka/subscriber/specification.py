from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.utils.path import Address
from faststream.kafka.configs import KafkaBrokerConfig
from faststream.kafka.subscriber.usecase import KAFKA_ADDRESS_SYNTAX
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, kafka

from .config import KafkaSubscriberSpecificationConfig


class KafkaSubscriberSpecification(
    SubscriberSpecification[KafkaBrokerConfig, KafkaSubscriberSpecificationConfig],
):
    @property
    def topics(self) -> list[str]:
        topics: set[str] = set()

        topics.update(f"{self._outer_config.prefix}{t}" for t in self.config.topics)

        topics.update(
            f"{self._outer_config.prefix}{p.topic}" for p in self.config.partitions
        )

        if self.config.pattern:
            # A topic is a literal and takes the prefix as one; a pattern is the
            # one Kafka argument that is compiled, so its prefix is compiled too.
            topics.add(
                Address(self.config.pattern, KAFKA_ADDRESS_SYNTAX)
                .add_prefix(self._outer_config.prefix)
                .template,
            )

        return list(topics)

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{','.join(self.topics)}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        channels = {}
        for t in self.topics:
            handler_name = self.config.title_ or f"{t}:{self.call_name}"

            channels[handler_name] = SubscriberSpec(
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{handler_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    kafka=kafka.ChannelBinding(
                        topic=t,
                        partitions=None,
                        replicas=None,
                    ),
                ),
            )

        return channels
