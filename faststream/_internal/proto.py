from copy import deepcopy
from typing import Any, overload

from typing_extensions import Self

from faststream._internal.utils.path import Address


class NameRequired:
    """Required name option object.

    The name is held as an `Address`, so that every address a value object carries
    is one type with one place to compose a Router prefix. Most names are read by
    their broker verbatim and get a verbatim Address; a name that is an Address
    template instead hands down the compiling Address it built.
    """

    address: Address
    """The name as it was declared, and the address it reaches the broker as."""

    def __eq__(self, value: object, /) -> bool:
        """Compares the current object with another object for equality."""
        if value is None:
            return False

        if not isinstance(value, NameRequired):
            return NotImplemented

        return self.name == value.name

    def __init__(self, name: "str | Address") -> None:
        self.address = name if isinstance(name, Address) else Address.literal(name)

    @property
    def name(self) -> str:
        """The name as it reaches the broker."""
        return self.address.broker_address

    def add_prefix(self, prefix: str) -> Self:
        """Return a copy of this object decorated with a Router prefix.

        The one object-level prefix pass, gathered here from the four that
        composed a prefix apiece. ADR-0006 has it going away rather than moving:
        once a placeholder is resolved per address field, the same branch that
        resolves a field prefixes it, and nothing is left for a pass over the
        whole object to do. Gathering it first is what makes that a deletion
        here instead of four.
        """
        new = deepcopy(self)
        new.address = new.address.add_prefix(prefix)
        return new

    @overload
    @classmethod
    def validate(cls, value: str | Self, **kwargs: Any) -> Self: ...

    @overload
    @classmethod
    def validate(cls, value: None, **kwargs: Any) -> None: ...

    @classmethod
    def validate(cls, value: str | Self | None, **kwargs: Any) -> Self | None:
        """Factory to create object."""
        if value is not None and isinstance(value, str):
            value = cls(value, **kwargs)
        return value
