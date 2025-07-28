from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Generic

from typing_extensions import (
    TypeVar as TypeVar313,
)

from faststream._internal.configs import BrokerConfig, SubscriberSpecificationConfig
from faststream.exceptions import SetupError
from faststream.specification.asyncapi.message import parse_handler_params
from faststream.specification.asyncapi.utils import to_camelcase

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import (
        CallsCollection,
    )
    from faststream.specification.schema import SubscriberSpec


T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=SubscriberSpecificationConfig,
    default=SubscriberSpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class SubscriberSpecification(Generic[T_BrokerConfig, T_SpecificationConfig]):
    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
        calls: "CallsCollection[Any]",
    ) -> None:
        self.calls = calls
        self.config = specification_config
        self._outer_config = _outer_config

    @property
    def include_in_schema(self) -> bool:
        return bool(
            self._outer_config.include_in_schema and self.config.include_in_schema,
        )

    @property
    def description(self) -> str | None:
        return self.config.description_ or self.calls.description

    @property
    def call_name(self) -> str:
        return self.calls.name or "Subscriber"

    def get_payloads(self) -> list[tuple["dict[str, Any]", str]]:
        payloads: list[tuple[dict[str, Any], str]] = []

        call_name = self.call_name

        for h in self.calls:
            if h.dependant is None:
                msg = "You should setup `Handler` at first."
                raise SetupError(msg)

            body = parse_handler_params(
                h.dependant,
                prefix=f"{self.config.title_ or call_name}:Message",
            )
            payloads.append((body, to_camelcase(h.name)))

        if not self.calls:
            payloads.append(
                (
                    {
                        "title": f"{self.config.title_ or call_name}:Message:Payload",
                    },
                    to_camelcase(call_name),
                ),
            )

        return payloads

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def get_schema(self) -> dict[str, "SubscriberSpec"]:
        raise NotImplementedError
