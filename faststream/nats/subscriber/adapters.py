from typing import Any, Generic, Optional, Protocol, TypeVar


class Unsubscriptable(Protocol):
    async def unsubscribe(self) -> None: ...


class Watchable(Protocol):
    async def stop(self) -> None: ...

    async def updates(self, timeout: float) -> Optional[Any]: ...


WatchableT = TypeVar("WatchableT", bound=Watchable)


class UnsubscribeAdapter(Unsubscriptable, Generic[WatchableT]):
    __slots__ = ("obj",)

    obj: WatchableT

    def __init__(self, subscription: WatchableT) -> None:
        self.obj = subscription

    async def unsubscribe(self) -> None:
        await self.obj.stop()
