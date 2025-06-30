from abc import abstractmethod
from typing import TYPE_CHECKING, Protocol

from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from aiokafka import AIOKafkaProducer


class ProducerState(Protocol):
    producer: "AIOKafkaProducer"

    @property
    @abstractmethod
    def closed(self) -> bool: ...

    def __bool__(self) -> bool: ...

    async def stop(self) -> None: ...

    async def flush(self) -> None: ...


class EmptyProducerState(ProducerState):
    __slots__ = ()

    closed = True

    @property
    def producer(self) -> "AIOKafkaProducer":
        msg = "You can't use producer here, please connect broker first."
        raise IncorrectState(msg)

    def __bool__(self) -> bool:
        return False

    async def stop(self) -> None:
        pass

    async def flush(self) -> None:
        pass


class RealProducer(ProducerState):
    __slots__ = ("producer",)

    def __init__(self, producer: "AIOKafkaProducer") -> None:
        self.producer = producer

    def __bool__(self) -> bool:
        return True

    async def stop(self) -> None:
        await self.producer.stop()

    @property
    def closed(self) -> bool:
        return self.producer._closed or False

    async def flush(self) -> None:
        await self.producer.flush()
