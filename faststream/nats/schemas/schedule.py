from datetime import datetime

from faststream._internal.proto import NameRequired


class Schedule(NameRequired):
    """A class to represent a message schedule."""

    __slots__ = ("target", "time")

    def __init__(self, time: datetime | str, target: str, /) -> None:
        super().__init__(target)
        self.time = datetime.fromisoformat(time) if isinstance(time, str) else time
        self.target = target
