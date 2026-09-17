from faststream._internal.endpoint.subscriber import SubscriberSpecification
from faststream._internal.utils.path import Address
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.schemas.js_stream import NATS_ADDRESS_SYNTAX
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, SubscriberSpec
from faststream.specification.schema.bindings import ChannelBinding, nats

from .config import NatsSubscriberSpecificationConfig


class NatsSubscriberSpecification(
    SubscriberSpecification[NatsBrokerConfig, NatsSubscriberSpecificationConfig],
):
    @property
    def subject(self) -> "Address":
        """The subject this endpoint was declared with, and its Broker address."""
        return Address(self.config.subject, NATS_ADDRESS_SYNTAX).add_prefix(
            self._outer_config.prefix,
        )

    @property
    def filter_subjects(self) -> list[str]:
        """The subjects a JetStream consumer filters on, and their Broker addresses."""
        return [
            Address(subject, NATS_ADDRESS_SYNTAX)
            .add_prefix(self._outer_config.prefix)
            .template
            for subject in self.config.filter_subjects
        ]

    @property
    def subjects(self) -> list[str]:
        """The subjects this endpoint reads, one channel each.

        A JetStream consumer with no `subject` reaches its stream through
        `filter_subjects`, and each of those is an address in its own right.
        """
        if subject := self.subject.template:
            return [subject]

        return self.filter_subjects

    @property
    def channel_labels(self) -> list[str]:
        return self.subjects

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        subjects = self.subjects
        split = len(subjects) > 1

        channels = {}
        for subject in subjects:
            name = self._channel_key(subject, split=split)

            channels[name] = SubscriberSpec(
                address=subject,
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=subject,
                        queue=self.config.queue,
                    ),
                ),
            )

        return channels


class NotIncludeSpecifation(SubscriberSpecification):
    @property
    def include_in_schema(self) -> bool:
        return False

    @property
    def channel_labels(self) -> list[str]:
        raise NotImplementedError

    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
