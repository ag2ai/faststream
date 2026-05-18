from abc import abstractmethod
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from typing import Any

from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.router import BrokerRouter


class BaseTestcaseConfig:
    timeout: float = 3.0

    @abstractmethod
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> BrokerUsecase[Any, Any]:
        raise NotImplementedError

    def patch_broker(
        self,
        *brokers: BrokerUsecase,
        **kwargs: Any,
    ) -> BrokerUsecase | list[BrokerUsecase]:
        if len(brokers) == 1:
            return brokers[0]

        @asynccontextmanager
        async def enter_broker() -> AsyncIterator[list[BrokerUsecase]]:
            started_brokers = []

            async with AsyncExitStack() as stack:
                for br in brokers:
                    started_brokers.append(await stack.enter_async_context(br))  # noqa: PERF401

                yield started_brokers

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
