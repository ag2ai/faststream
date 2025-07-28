from typing import Annotated, Any, TypeVar

from fast_depends.library import CustomField


class NoCastField(CustomField):
    """A class that represents a custom field without casting.

    You can use it to annotate fields, that should not be casted.

    Usage:

    `data: Annotated[..., NoCast()]`
    """

    def __init__(self) -> None:
        super().__init__(cast=False)

    def use(self, **kwargs: Any) -> dict[str, Any]:
        return kwargs


_NoCastType = TypeVar("_NoCastType")

NoCast = Annotated[_NoCastType, NoCastField()]
