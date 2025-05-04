from collections.abc import Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Optional

from faststream._internal.constants import EMPTY

if TYPE_CHECKING:
    from fast_depends import Provider
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import Decorator
    from faststream._internal.context import ContextRepo


@dataclass
class DIState:
    use_fastdepends: bool
    get_dependent: Optional[Callable[..., Any]]
    call_decorators: Sequence["Decorator"]
    provider: "Provider"
    serializer: Optional["SerializerProto"]
    context: "ContextRepo"

    def update(
        self,
        *,
        provider: "Provider" = EMPTY,
        serializer: Optional["SerializerProto"] = EMPTY,
        context: "ContextRepo" = EMPTY,
    ) -> None:
        if provider is not EMPTY:
            self.provider = provider

        if serializer is not EMPTY:
            self.serializer = serializer

        if context is not EMPTY:
            self.context = context
