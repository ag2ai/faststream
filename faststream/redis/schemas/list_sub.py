from copy import deepcopy
from functools import cached_property

from faststream._internal.proto import NameRequired


class ListSub(NameRequired):
    """A class to represent a Redis List subscriber."""

    __slots__ = (
        "batch",
        "max_records",
        "name",
        "polling_interval",
    )

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

    @classmethod
    def from_config_value(cls, value: "ListSub | str", config_key: str) -> "ListSub":
        """Build this list out of a Config value.

        A list name reaches Redis verbatim — no Address template compiles here —
        so there is nothing for the Config key to explain, and it is dropped.
        """
        return cls.validate(value)

    @cached_property
    def records(self) -> int | None:
        return self.max_records if self.batch else None

    def add_prefix(self, prefix: str) -> "ListSub":
        new_list = deepcopy(self)
        new_list.name = f"{prefix}{new_list.name}"
        return new_list
