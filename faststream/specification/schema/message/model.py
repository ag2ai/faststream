from dataclasses import dataclass
from typing import Any


@dataclass
class Message:
    payload: dict[str, Any]  # JSON Schema

    title: str | None
