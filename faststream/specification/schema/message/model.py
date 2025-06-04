from dataclasses import dataclass
from typing import Optional

from faststream._internal.basic_types import AnyDict


@dataclass
class Message:
    payload: AnyDict  # JSON Schema

    title: Optional[str]
