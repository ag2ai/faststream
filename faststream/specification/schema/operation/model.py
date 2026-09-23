from dataclasses import dataclass

from faststream.specification.schema.bindings import OperationBinding
from faststream.specification.schema.message import Message


@dataclass
class Operation:
    message: Message
    bindings: OperationBinding | None
