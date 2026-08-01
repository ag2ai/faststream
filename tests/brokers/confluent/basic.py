from contextlib import AbstractAsyncContextManager
from typing import Any, overload

from typing_extensions import override

from faststream.confluent import (
    KafkaBroker,
    KafkaRouter,
    TestKafkaBroker,
    TopicPartition,
)
from tests.brokers.base.basic import BaseTestcaseConfig


class ConfluentTestcaseConfig(BaseTestcaseConfig[KafkaBroker]):
    timeout: float = 10.0

    def get_subscriber_params(
        self,
        *topics: Any,
        **kwargs: Any,
    ) -> tuple[
        tuple[Any, ...],
        dict[str, Any],
    ]:
        if len(topics) == 1:
            partitions = [TopicPartition(topics[0], partition=0, offset=0)]
            topics = ()

        else:
            partitions = []

        return topics, {
            "auto_offset_reset": "earliest",
            "partitions": partitions,
            **kwargs,
        }

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> KafkaBroker:
        return KafkaBroker(apply_types=apply_types, **kwargs)

    def get_router(self, **kwargs: Any) -> KafkaRouter:
        return KafkaRouter(**kwargs)


class ConfluentMemoryTestcaseConfig(ConfluentTestcaseConfig):
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
