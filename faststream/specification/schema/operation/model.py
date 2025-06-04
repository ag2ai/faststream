from dataclasses import dataclass
from typing import Optional

from faststream.specification.schema.bindings import OperationBinding
from faststream.specification.schema.message import Message


@dataclass
class Operation:
    message: Message
    bindings: Optional[OperationBinding]
