from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class Schedule:
    """A class to represent a message schedule."""

    time: datetime
    target: str
