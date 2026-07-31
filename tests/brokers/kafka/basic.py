from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.kafka import KafkaBroker, KafkaRouter, TestKafkaBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class KafkaTestcaseConfig(BaseTestcaseConfig[KafkaBroker]):
    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> KafkaRouter:
        return KafkaRouter(**kwargs)


class KafkaMemoryTestcaseConfig(KafkaTestcaseConfig):
    @overload
    def patch_broker(
        self,
        brokers: KafkaBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[KafkaBroker]: ...

    @overload
    def patch_broker(
        self,
        *brokers: KafkaBroker,
        **kwargs: Any,
    ) -> AbstractAsyncContextManager[tuple[KafkaBroker, ...]]: ...

    @override
    def patch_broker(
        self,
        *brokers: KafkaBroker,
        **kwargs: Any,
    ) -> Any:
        return TestKafkaBroker(*brokers, **kwargs)
