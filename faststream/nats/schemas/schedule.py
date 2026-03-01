from dataclasses import dataclass
from datetime import datetime


@dataclass
class Schedule:
    """A class to represent a message schedule."""

    time: datetime
    target: str
