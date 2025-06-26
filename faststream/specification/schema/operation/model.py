from typing import Optional
from dataclasses import dataclass

from faststream.specification.schema.bindings import OperationBinding
from faststream.specification.schema.message import Message


@dataclass
class Operation:
    operationId: str
    message: Message
    bindings: OperationBinding | None
