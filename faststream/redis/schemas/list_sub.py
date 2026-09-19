from copy import deepcopy
from functools import cached_property
from typing import Generic, Literal, overload

from typing_extensions import Self, TypeVar

from faststream._internal.proto import NameRequired

# Carries `batch` in the type, so `subscriber(list=ListSub(..., batch=True))`
# resolves to the batch subscriber instead of a Union of both.
BatchT_co = TypeVar("BatchT_co", bound=bool, default=bool, covariant=True)


class ListSub(NameRequired, Generic[BatchT_co]):
    """A class to represent a Redis List subscriber."""

    __slots__ = (
        "batch",
        "max_records",
        "name",
        "polling_interval",
    )

    @overload
    def __init__(
        self: "ListSub[Literal[False]]",
        list_name: str,
        batch: Literal[False] = False,
        max_records: int = 10,
        polling_interval: float = 0.1,
    ) -> None: ...

    @overload
    def __init__(
        self: "ListSub[Literal[True]]",
        list_name: str,
        batch: Literal[True],
        max_records: int = 10,
        polling_interval: float = 0.1,
    ) -> None: ...

    @overload
    def __init__(
        self: "ListSub[bool]",
        list_name: str,
        batch: bool,
        max_records: int = 10,
        polling_interval: float = 0.1,
    ) -> None: ...

    def __init__(
        self,
        list_name: str,
        batch: bool = False,
        max_records: int = 10,
        polling_interval: float = 0.1,
    ) -> None:
        super().__init__(list_name)

        self.batch = batch
        self.max_records = max_records
        self.polling_interval = polling_interval

    @cached_property
    def records(self) -> int | None:
        return self.max_records if self.batch else None

    def add_prefix(self, prefix: str) -> Self:
        new_list = deepcopy(self)
        new_list.name = f"{prefix}{new_list.name}"
        return new_list
