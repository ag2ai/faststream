from dataclasses import dataclass

from faststream.specification.schema import Message


@dataclass
class OperationReplyAddress:
    location: str
    description: str | None = None


@dataclass
class OperationReply:
    message: Message | None
    address: OperationReplyAddress | None
