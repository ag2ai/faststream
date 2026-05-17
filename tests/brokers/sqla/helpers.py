from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass
class Settings:
    engine: AsyncEngine


def as_datetime(value: datetime | str) -> datetime:
    match value:
        case datetime():
            return value
        case str():
            return datetime.fromisoformat(value)
        case _:
            raise ValueError
