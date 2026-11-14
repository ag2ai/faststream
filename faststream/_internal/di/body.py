from inspect import Parameter
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fast_depends.core import CallModel
    from fast_depends.library.serializer import OptionItem


class BodyBinding:
    """External body parameters, without dependency results or custom fields."""

    __slots__ = ("params",)

    def __init__(self, call: "CallModel") -> None:
        self.params = tuple(call.flat_params)

    @property
    def single_parameter(self) -> "OptionItem | None":
        return self.params[0] if len(self.params) == 1 else None

    @property
    def unpack(self) -> bool:
        single = self.single_parameter
        # A lone *args parameter consumes the body's sequence elements.
        return len(self.params) > 1 or (
            single is not None and single.kind is Parameter.VAR_POSITIONAL
        )
