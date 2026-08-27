from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream.confluent.configs import KafkaBrokerConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, kafka

from .config import KafkaSubscriberSpecificationConfig


class KafkaSubscriberSpecification(
    SubscriberSpecification[KafkaBrokerConfig, KafkaSubscriberSpecificationConfig],
):
    @property
    def topics(self) -> list[str]:
        """The topics this endpoint reads, in the order they were declared.

        Deduped through a dict rather than a set: set order varies per process and
        would reach the document as the order of its channels.
        """
        prefix = self._outer_config.prefix

        topics = [f"{prefix}{t.name}" for t in self.config.topics]
        topics.extend(f"{prefix}{p.topic}" for p in self.config.partitions)

        return list(dict.fromkeys(topics))

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{','.join(self.topics)}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        topics = self.topics
        split = len(topics) > 1

        channels = {}
        for t in topics:
            handler_name = self._channel_key(t, split=split)

            channels[handler_name] = SubscriberSpec(
                address=t,
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
