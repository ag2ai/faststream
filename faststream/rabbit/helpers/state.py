from dataclasses import dataclass
from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from aio_pika import RobustConnection


class ConnectionState(Protocol):
    @property
    def connection(self) -> "RobustConnection": ...


class EmptyConnectionState:
    __slots__ = ()

    @property
    def connection(self) -> "RobustConnection":
        msg = "You should connect broker first."
        raise IncorrectState(msg)


@dataclass(slots=True)
class ConnectedState:
    connection: "RobustConnection"
