from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream.confluent.helpers.client import AsyncConfluentProducer


class ProducerState(Protocol):
    @property
    def producer(self) -> "AsyncConfluentProducer": ...

    def __bool__(self) -> bool: ...

    async def ping(self, timeout: float) -> bool: ...

    async def stop(self) -> None: ...

    async def flush(self) -> None: ...


class EmptyProducerState:
    __slots__ = ()

    @property
    def producer(self) -> "AsyncConfluentProducer":
        msg = "You can't use producer here, please connect broker first."
        raise IncorrectState(msg)

    async def ping(self, timeout: float) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    async def stop(self) -> None:
        pass

    async def flush(self) -> None:
        pass


class RealProducer:
    __slots__ = ("producer",)

    def __init__(self, producer: "AsyncConfluentProducer") -> None:
        self.producer = producer

    def __bool__(self) -> bool:
        return True

    async def stop(self) -> None:
        await self.producer.stop()

    async def ping(self, timeout: float) -> bool:
        return await self.producer.ping(timeout=timeout)

    async def flush(self) -> None:
        await self.producer.flush()
