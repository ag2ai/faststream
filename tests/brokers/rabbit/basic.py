from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.rabbit import RabbitBroker, RabbitRouter, TestRabbitBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class RabbitTestcaseConfig(BaseTestcaseConfig[RabbitBroker]):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> RabbitBroker:
        return RabbitBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> RabbitRouter:
        return RabbitRouter(**kwargs)


class RabbitMemoryTestcaseConfig(RabbitTestcaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: RabbitBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[RabbitBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: RabbitBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[RabbitBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: RabbitBroker,
        **kwargs: Any,
    ) -> Any:
        return TestRabbitBroker(*brokers, **kwargs)
