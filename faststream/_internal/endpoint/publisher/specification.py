from inspect import Parameter, unwrap
from typing import TYPE_CHECKING, Any, Generic

from fast_depends.core import build_call_model
from fast_depends.pydantic._compat import create_model, get_config_base
from typing_extensions import TypeVar as TypeVar313

from faststream._internal.configs import BrokerConfig, PublisherSpecificationConfig
from faststream.specification.asyncapi.message import get_model_schema
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


class PublisherSpecification(Generic[T_BrokerConfig, T_SpecificationConfig]):
    def __init__(
        self,
        _outer_config: "T_BrokerConfig",
        specification_config: "T_SpecificationConfig",
    ) -> None:
        self.config = specification_config
        self._outer_config = _outer_config

        self.calls: list[AnyCallable] = []

    def add_call(self, call: "AnyCallable") -> None:
        self.calls.append(call)

    @property
    def include_in_schema(self) -> bool:
        return bool(
            self._outer_config.include_in_schema and self.config.include_in_schema,
        )

    def get_payloads(self) -> list[tuple[dict[str, Any], str]]:
        payloads: list[tuple[dict[str, Any], str]] = []

        if self.config.schema_:
            body = get_model_schema(
                call=create_model(
                    "",
                    __config__=get_config_base(),
                    response__=(self.config.schema_, ...),
                ),
                prefix=f"{self.name}:Message",
            )

            if body:  # pragma: no branch
                payloads.append((body, ""))

        else:
            di_state = self._outer_config.fd_config

            for call in self.calls:
                call_model = build_call_model(
                    call,
                    dependency_provider=di_state.provider,
                    serializer_cls=di_state._serializer,
                )

                if call_model.serializer:
                    response_type = next(
                        iter(call_model.serializer.response_option.values()),
                    ).field_type
                else:
                    response_type = None

                if response_type is not None and response_type is not Parameter.empty:
                    body = get_model_schema(
                        create_model(
                            "",
                            __config__=get_config_base(),
                            response__=(response_type, ...),
                        ),
                        prefix=f"{self.name}:Message",
                    )

                    if body:
                        payloads.append((body, to_camelcase(unwrap(call).__name__)))

        return payloads

    @property
    def name(self) -> str:
        raise NotImplementedError

    def get_schema(self) -> dict[str, "PublisherSpec"]:
        raise NotImplementedError
