from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from aio_pika import RobustConnection


class ConnectionState(Protocol):
    connection: "RobustConnection"


class EmptyConnectionState(ConnectionState):
    __slots__ = ()

    error_msg = "You should connect broker first."

    @property
    def connection(self) -> "RobustConnection":
        raise IncorrectState(self.error_msg)


class ConnectedState(ConnectionState):
    __slots__ = ("connection",)

    def __init__(
        self, connection: "RobustConnection"
    ) -> None:
        self.connection = connection
