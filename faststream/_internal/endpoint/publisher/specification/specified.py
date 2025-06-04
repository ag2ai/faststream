from inspect import Parameter, unwrap
from typing import TYPE_CHECKING, Any, Callable, Union

from fast_depends.core import build_call_model
from fast_depends.pydantic._compat import create_model, get_config_base

from faststream._internal.endpoint.specification.base import SpecificationEndpoint
from faststream._internal.types import (
    MsgType,
    P_HandlerParams,
    T_HandlerReturn,
)
from faststream.specification.asyncapi.message import get_model_schema
from faststream.specification.asyncapi.utils import to_camelcase
from faststream.specification.schema import PublisherSpec

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyCallable, AnyDict
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper

    from .config import PublisherSpecificationConfig


class SpecificationPublisher(SpecificationEndpoint[MsgType, PublisherSpec]):
    def __init__(
        self,
        config: "PublisherSpecificationConfig",
        **kwargs: Any,
    ) -> None:
        self.calls: list[AnyCallable] = []
        self.schema_ = config.schema_
        super().__init__(config, **kwargs)

    def __call__(
        self,
        func: Union[
            Callable[P_HandlerParams, T_HandlerReturn],
            "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]",
        ],
    ) -> "HandlerCallWrapper[MsgType, P_HandlerParams, T_HandlerReturn]":
        handler = super().__call__(func)
        self.calls.append(handler._original_call)
        return handler

    def get_payloads(self) -> list[tuple["AnyDict", str]]:
        payloads: list[tuple[AnyDict, str]] = []

        if self.schema_:
            body = get_model_schema(
                call=create_model(
                    "",
                    __config__=get_config_base(),
                    response__=(self.schema_, ...),
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

                response_type = next(
                    iter(call_model.serializer.response_option.values())
                ).field_type
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
