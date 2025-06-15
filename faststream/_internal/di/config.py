from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Optional

from fast_depends import Provider

from faststream._internal.constants import EMPTY
from faststream._internal.context import ContextRepo

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import Decorator


@dataclass(kw_only=True)
class FastDependsConfig:
    use_fastdepends: bool = True

    provider: Optional["Provider"] = field(default_factory=Provider)
    serializer: Optional["SerializerProto"] = field(default_factory=lambda: EMPTY)

    context: "ContextRepo" = field(default_factory=ContextRepo)

    # To patch injection by integrations
    call_decorators: Sequence["Decorator"] = ()
    get_dependent: Callable[..., Any] | None = None

    @property
    def _serializer(self) -> Optional["SerializerProto"]:
        if self.serializer is EMPTY:
            from fast_depends.pydantic import PydanticSerializer

            return PydanticSerializer()

        return self.serializer

    def __or__(self, value: "FastDependsConfig", /) -> "FastDependsConfig":
        use_fd = False if not value.use_fastdepends else self.use_fastdepends

        return FastDependsConfig(
            use_fastdepends=use_fd,
            provider=value.provider or self.provider,
            serializer=self.serializer or value.serializer,
            context=self.context,
            call_decorators=(*value.call_decorators, *self.call_decorators),
            get_dependent=self.get_dependent or value.get_dependent,
        )
