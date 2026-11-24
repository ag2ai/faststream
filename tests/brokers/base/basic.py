from abc import abstractmethod
from collections.abc import AsyncGenerator
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import Any, Generic, overload

from typing_extensions import TypeVar

from faststream._internal.broker import BrokerUsecase

_BrokerT = TypeVar(
    "_BrokerT",
    bound=BrokerUsecase[Any, Any, Any],
    default=BrokerUsecase[Any, Any, Any],
)


class BaseTestcaseConfig(Generic[_BrokerT]):
    timeout: float = 3.0
    nack_is_observable: bool = True
    """Whether a nack reaches `StreamMessage.nack`; MQTT has none to send."""

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
        async def enter_broker() -> AsyncGenerator[tuple[Any, ...], None]:
            started_brokers: list[Any] = []

            async with AsyncExitStack() as stack:
                for br in brokers:
                    started_brokers.append(await stack.enter_async_context(br))  # ruff: ignore[manual-list-comprehension]

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

    def get_ack_subscriber_params(
        self,
        queue: str,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """A subscriber that settles: Kafka needs a group, NATS and Redis a stream."""
        return self.get_subscriber_params(queue, **kwargs)

    async def publish_to_ack_subscriber(
        self,
        broker: Any,
        queue: str,
        message: str,
    ) -> None:
        """Reach `get_ack_subscriber_params`; NATS and Redis address the stream."""
        await broker.publish(message, queue)

    @abstractmethod
    def get_router(self, **kwargs: Any) -> Any:
        raise NotImplementedError
