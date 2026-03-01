from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


@dataclass
class Queue:
    name: str
    durable: bool
    exclusive: bool
    auto_delete: bool

    @classmethod
    def from_queue(cls, queue: "RabbitQueue") -> "Queue":
        return cls(
            name=queue.name,
            durable=queue.durable,
            exclusive=queue.exclusive,
            auto_delete=queue.auto_delete,
        )


@dataclass
class Exchange:
    type: Literal[
        "default",
        "direct",
        "topic",
        "fanout",
        "headers",
        "x-delayed-message",
        "x-consistent-hash",
        "x-modulus-hash",
    ]

    name: str | None = None
    durable: bool | None = None
    auto_delete: bool | None = None

    @classmethod
    def from_exchange(cls, exchange: "RabbitExchange") -> "Exchange":
        if not exchange.name:
            return cls(type="default")
        return cls(
            type=exchange.type.value,
            name=exchange.name,
            durable=exchange.durable,
            auto_delete=exchange.auto_delete,
        )

    @property
    def is_respect_routing_key(self) -> bool:
        """Is exchange respects routing key or not."""
        return self.type in {
            "default",
            "direct",
            "topic",
        }


@dataclass
class ChannelBinding:
    queue: Queue
    exchange: Exchange
    virtual_host: str


@dataclass
class OperationBinding:
    routing_key: str | None
    queue: Queue
    exchange: Exchange
    ack: bool
    reply_to: str | None
    persist: bool | None
    mandatory: bool | None
    priority: int | None
