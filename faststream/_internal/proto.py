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

    _address: Address

    def __eq__(self, value: object, /) -> bool:
        """Compares the current object with another object for equality."""
        if value is None:
            return False

        if not isinstance(value, NameRequired):
            return NotImplemented

        return self.name == value.name

    def __init__(self, name: "str | Address") -> None:
        self._address = name if isinstance(name, Address) else Address.literal(name)

    @property
    def address(self) -> Address:
        """The name as it was declared, and the address it reaches the broker as.

        Read-only, and so is every other address a value object holds. ADR-0005
        makes the type parameter over it covariant, which is unsound over a
        mutable attribute — mypy does not flag that outside a protocol, so the
        attribute being unwriteable is what holds it up rather than the checker.
        """
        return self._address

    @property
    def name(self) -> str:
        """The name as it reaches the broker."""
        return self._address.broker_address

    def _with_prefix(self, prefix: str) -> Self:
        """A copy of this object whose name carries a Router prefix.

        Protected, because a prefix does not reach every value object: an
        exchange, a stream and a bucket are named outside the Router's
        namespace, and a public `add_prefix` on them would make the wrong call
        type-check. The four that are decorated expose one that lands here, so
        the composition itself still happens in a single place.
        """
        new = deepcopy(self)
        new._address = new._address.add_prefix(prefix)
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
