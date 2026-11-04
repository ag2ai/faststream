from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class Message:
    payload: dict[str, Any]  # JSON Schema

    title: str | None
