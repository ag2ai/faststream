from abc import abstractmethod
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import Any, Generic, overload

from typing_extensions import TypeVar

from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.router import BrokerRouter

_BrokerT = TypeVar("_BrokerT", bound=BrokerUsecase[Any, Any], default=Any)


class BaseTestcaseConfig(Generic[_BrokerT]):
    timeout: float = 3.0

    @abstractmethod
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> _BrokerT:
        raise NotImplementedError

    @overload
    def patch_broker(
        self,
        brokers: _BrokerT,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[_BrokerT]: ...

    @overload
    def patch_broker(
        self,
        *brokers: _BrokerT,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[_BrokerT, ...]]: ...

    def patch_broker(
        self,
        *brokers: _BrokerT,
        **kwargs: Any,
    ) -> Any:
        if len(brokers) == 1:
            return brokers[0]

        @asynccontextmanager
        async def enter_broker() -> AsyncGenerator[tuple[BrokerUsecase, ...], None]:
            started_brokers = []

            async with AsyncExitStack() as stack:
                for br in brokers:
                    started_brokers.append(await stack.enter_async_context(br))  # noqa: PERF401

                yield tuple(started_brokers)

        return enter_broker()

    def get_subscriber_params(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        return args, kwargs

    @abstractmethod
    def get_router(self, **kwargs: Any) -> BrokerRouter:
        raise NotImplementedError
