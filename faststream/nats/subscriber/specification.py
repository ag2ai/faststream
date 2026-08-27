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
    def _resolved_subject_string(self) -> str:
        """The declared subject, falling back to the filtered subjects when there is none.

        A JetStream consumer can address a stream through `filter_subjects` alone, leaving
        `subject` empty. Mirrors `LogicSubscriber._resolved_subject_string`.
        """
        return self.subject.template or ", ".join(self.filter_subjects)

    @property
    def address(self) -> str | None:
        """The subject a publisher sends to for this endpoint to receive it.

        A JetStream consumer with no `subject` reaches the stream through
        `filter_subjects`, and a single one of those is that address. Several are
        not: no one string is the address, so the document gives none.
        """
        if subject := self.subject.template:
            return subject

        if len(subjects := self.filter_subjects) == 1:
            return subjects[0]

        return None

    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_

        return f"{self._resolved_subject_string}:{self.call_name}"

    def get_schema(self) -> dict[str, SubscriberSpec]:
        payloads = self.get_payloads()

        return {
            self.name: SubscriberSpec(
                address=self.address,
                description=self.description,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                    bindings=None,
                ),
                bindings=ChannelBinding(
                    nats=nats.ChannelBinding(
                        subject=self._resolved_subject_string,
                        queue=self.config.queue,
                    ),
                ),
            ),
        }


class NotIncludeSpecifation(SubscriberSpecification):
    @property
    def include_in_schema(self) -> bool:
        return False

    @property
    def name(self) -> str:
        raise NotImplementedError

    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
