from dataclasses import dataclass

from faststream._internal.basic_types import AnyDict


@dataclass
class Message:
    payload: AnyDict  # JSON Schema

    title: str | None
