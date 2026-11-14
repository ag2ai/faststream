from inspect import Parameter, unwrap
from typing import TYPE_CHECKING, Any

from fast_depends.core import build_call_model
from typing_extensions import TypeVar as TypeVar313

from faststream._internal.configs import BrokerConfig, PublisherSpecificationConfig
from faststream._internal.endpoint.specification import EndpointSpecification
from faststream.exceptions import SetupError
from faststream.specification.asyncapi.message import payload_schema
from faststream.specification.asyncapi.utils import to_camelcase

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyCallable
    from faststream.specification.schema import PublisherSpec


T_SpecificationConfig = TypeVar313(
    "T_SpecificationConfig",
    bound=PublisherSpecificationConfig,
    default=PublisherSpecificationConfig,
)
T_BrokerConfig = TypeVar313("T_BrokerConfig", bound=BrokerConfig, default=BrokerConfig)


class PublisherSpecification(
    EndpointSpecification[T_BrokerConfig, T_SpecificationConfig],
):
    __slots__ = ("calls",)

    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
    ) -> None:
        super().__init__(_outer_config, specification_config)

        self.calls: list[AnyCallable] = []

    def add_call(self, call: "AnyCallable") -> None:
        self.calls.append(call)

    def get_payloads(self) -> list[tuple[dict[str, Any], str]]:
        payloads: list[tuple[dict[str, Any], str]] = []

        di_state = self._outer_config.fd_config
        factory = di_state._serializer
        prefix = f"{self.name}:Message"

        if self.config.schema_ is not None:
            if factory is None:
                msg = "AsyncAPI requires a serializer with JSON Schema support."
                raise SetupError(msg)
            body = payload_schema(factory, self.config.schema_, prefix=prefix)
            payloads.append((body, ""))
        else:
            for call in self.calls:
                call_model = build_call_model(
                    call,
                    dependency_provider=di_state.provider,
                    serializer_cls=factory,
                )
                serializer = call_model.serializer
                if serializer is None:
                    continue
                response_type = serializer.response_option["return"].field_type
                if (
                    factory is not None
                    and response_type is not None
                    and response_type is not Parameter.empty
                ):
                    body = payload_schema(factory, response_type, prefix=prefix)
                    payloads.append((body, to_camelcase(unwrap(call).__name__)))

        return payloads

    @property
    def name(self) -> str:
        raise NotImplementedError

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        raise NotImplementedError
