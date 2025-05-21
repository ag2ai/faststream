from typing import TYPE_CHECKING, Generic, TypeVar

from typing_extensions import Self

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict

T = TypeVar("T")


class Pointer(Generic[T]):
    __slots__ = ("__value",)

    def __init__(self, value: T) -> None:
        self.__value = value

    def set(self, new_value: T) -> "Self":
        self.__value = new_value
        return self

    def get(self) -> T:
        return self.__value

    def patch_value(self, **kwargs: "AnyDict") -> None:
        for k, v in kwargs.items():
            setattr(self.__value, k, v)
