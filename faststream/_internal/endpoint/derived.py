from typing import Generic, Protocol, TypeVar

from faststream.exceptions import IncorrectState


class ResettableRead(Protocol):
    """A value derived from the options composition and kept until it is undone."""

    def reset(self) -> None:
        """Forget the derived value, so the next read derives it again."""
        ...


ReadT = TypeVar("ReadT", bound=ResettableRead)
T = TypeVar("T")


class DerivedReads:
    """Every read an object keeps from the options composition.

    Registering a read here is the whole of what it takes for it to be forgotten
    when Preparation is undone. The alternative is an `invalidate` override in
    every class that grows a read — a second edit in a second method, and the
    second is the one that gets forgotten.

    Itself a `ResettableRead`, so an object holding a collection of them can
    hand it over as one.
    """

    __slots__ = ("_reads",)

    def __init__(self) -> None:
        self._reads: list[ResettableRead] = []

    def add(self, read: ReadT) -> ReadT:
        """Register a read, and hand it back for the caller to store."""
        self._reads.append(read)
        return read

    def reset(self) -> None:
        """Forget every registered read."""
        for read in self._reads:
            read.reset()


class Resolved(Generic[T]):
    """What Preparation resolved, written once and read as a field afterwards.

    An endpoint's addresses depend on the Router prefix and on the Config values
    in scope, neither of which is final until the composition is. Answering
    before then means answering from an incomplete composition, so this refuses
    instead: the caller asked too early, and the fix is to ask later rather than
    to change the declaration.

    A `ResettableRead`, so registering it with `DerivedReads` is all it takes for
    the next Preparation to resolve again.
    """

    __slots__ = ("_held", "_subject")

    def __init__(self, subject: str) -> None:
        self._subject = subject
        """What is being read, named the way the error message should name it."""

        # Wrapped rather than stored beside a sentinel, so that `None` can be a
        # resolved value in its own right -- a Subscriber with no group, say.
        self._held: tuple[T] | None = None

    def set(self, value: T) -> None:
        """Write what Preparation resolved."""
        self._held = (value,)

    def get(self) -> T:
        """Answer with it, or refuse a read that came before Preparation."""
        if self._held is None:
            msg = (
                f"Reading {self._subject} before Preparation: the read came too "
                "early. An endpoint resolves its options when its Broker is "
                "prepared — at `connect()`, at `App.start()`, or in the "
                "endpoint's own `start()` — and answers every read from then on. "
                "A schema render prepares too, but only for as long as it lasts, "
                "because it opens no connection to hold the resolved values."
            )
            raise IncorrectState(msg)

        return self._held[0]

    def reset(self) -> None:
        """Forget it, so the next Preparation resolves again."""
        self._held = None
