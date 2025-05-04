from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
)

from faststream._internal.types import MsgType
from faststream.exceptions import SetupError
from faststream.specification.asyncapi.message import parse_handler_params
from faststream.specification.asyncapi.utils import to_camelcase
from faststream.specification.proto import EndpointSpecification
from faststream.specification.schema import SubscriberSpec

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict

    from .call_item import HandlerItem


class SpecificationSubscriber(EndpointSpecification[MsgType, SubscriberSpec]):
    calls: list["HandlerItem[MsgType]"]

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        self.calls = []

        # Call next base class parent init
        super().__init__(*args, **kwargs)

    @property
    def call_name(self) -> str:
        """Returns the name of the handler call."""
        if not self.calls:
            return "Subscriber"

        if len(self.calls) == 1:
            return to_camelcase(self.calls[0].call_name)

        return f"[{','.join(to_camelcase(c.call_name) for c in self.calls)}]"

    def get_default_description(self) -> Optional[str]:
        """Returns the description of the handler."""
        if not self.calls:
            return None

        if len(self.calls) == 1:
            return self.calls[0].description

        return "\n".join(
            f"{to_camelcase(h.call_name)}: {h.description or ''}" for h in self.calls
        )

    def get_payloads(self) -> list[tuple["AnyDict", str]]:
        """Get the payloads of the handler."""
        payloads: list[tuple[AnyDict, str]] = []

        for h in self.calls:
            if h.dependant is None:
                msg = "You should setup `Handler` at first."
                raise SetupError(msg)

            body = parse_handler_params(
                h.dependant,
                prefix=f"{self.title_ or self.call_name}:Message",
            )

            payloads.append((body, to_camelcase(h.call_name)))

        if not self.calls:
            payloads.append(
                (
                    {
                        "title": f"{self.title_ or self.call_name}:Message:Payload",
                    },
                    to_camelcase(self.call_name),
                ),
            )

        return payloads
