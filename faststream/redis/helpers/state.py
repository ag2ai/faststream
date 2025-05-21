from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from redis.asyncio.client import Redis


class ConnectionState(Protocol):
    client: "Redis[bytes]"


class EmptyConnectionState(ConnectionState):
    __slots__ = ()

    error_msg = "You should connect broker first."

    @property
    def client(self) -> "Redis[bytes]":
        raise IncorrectState(self.error_msg)


class ConnectedState(ConnectionState):
    __slots__ = ("client",)

    def __init__(self, client: "Redis[bytes]") -> None:
        self.client = client
