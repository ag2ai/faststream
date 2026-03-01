import inspect
from collections.abc import Awaitable, Callable, Mapping, Reversible, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from fast_depends import Provider, dependency_provider
from fast_depends.core import CallModel, build_call_model

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo
from faststream._internal.utils import apply_types, to_async

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from fast_depends.use import InjectWrapper

    from faststream._internal.basic_types import Decorator
    from faststream.message import StreamMessage


@dataclass(kw_only=True)
class BuiltDependant:
    original_call: Callable[..., Any]
    wrapped_call: Callable[..., Any]
    dependent: "CallModel"


@dataclass(kw_only=True)
class FastDependsConfig:
    use_fastdepends: bool = True

    provider: "Provider" = field(default=dependency_provider)
    serializer: Optional["SerializerProto"] = field(default_factory=lambda: EMPTY)

    context: "ContextRepo" = field(default_factory=ContextRepo)

    # To patch injection by integrations
    call_decorators: Sequence["Decorator"] = ()
    get_dependent: Callable[..., Any] | None = None

    @property
    def _serializer(self) -> Optional["SerializerProto"]:
        if self.serializer is EMPTY:
            from fast_depends.pydantic import PydanticSerializer

            return PydanticSerializer(use_fastdepends_errors=False)

        return self.serializer

    def __or__(self, value: "FastDependsConfig", /) -> "FastDependsConfig":
        use_fd = False if not value.use_fastdepends else self.use_fastdepends

        return FastDependsConfig(
            use_fastdepends=use_fd,
            provider=value.provider,
            serializer=self.serializer or value.serializer,
            context=self.context,
            call_decorators=(*value.call_decorators, *self.call_decorators),
            get_dependent=self.get_dependent or value.get_dependent,
        )

    def build_call(
        self,
        call: Callable[..., Any],
        *,
        dependencies: Sequence["Dependant"] = (),
        call_decorators: Reversible["Decorator"] = (),
    ) -> BuiltDependant:
        for d in reversed((*call_decorators, *self.call_decorators)):
            call = d(call)

        wrapped_call: Callable[..., Awaitable[Any]] = to_async(call)

        if self.get_dependent:
            dependent = self.get_dependent(wrapped_call, dependencies)

        else:
            dependent = build_call_model(
                wrapped_call,
                extra_dependencies=dependencies,
                dependency_provider=self.provider,
                serializer_cls=self._serializer,
            )

            if self.use_fastdepends:
                wrapper: InjectWrapper[..., Any] = apply_types(
                    None, context__=self.context
                )
                wrapped_call = wrapper(func=wrapped_call, model=dependent)

            wrapped_call = _unwrap_message_to_fast_depends_decorator(
                wrapped_call,
                dependent,
            )

        return BuiltDependant(
            original_call=call,
            wrapped_call=wrapped_call,
            dependent=dependent,
        )


def _unwrap_message_to_fast_depends_decorator(
    func: Callable[..., Any],
    dependent: "CallModel",
) -> Callable[["StreamMessage[Any]"], Awaitable[Any]]:
    dependant_params = dependent.flat_params
    if len(dependant_params) <= 1:
        if option := next(iter(dependant_params), None):
            is_multi_params = option.kind is inspect.Parameter.VAR_POSITIONAL
        else:
            is_multi_params = False
    else:
        is_multi_params = True

    if is_multi_params:

        async def decode_wrapper(message: "StreamMessage[Any]") -> Any:
            msg = await message.decode()

            if isinstance(msg, Mapping):
                return await func(**msg)

            if isinstance(msg, Sequence):
                return await func(*msg)

            msg = f"Couldn't unpack `{msg}` to multiple values."
            raise ValueError(msg)

    else:

        async def decode_wrapper(message: "StreamMessage[Any]") -> Any:
            msg = await message.decode()
            return await func(msg)

    return decode_wrapper
