import warnings
from abc import abstractmethod
from collections.abc import AsyncGenerator, Generator, Iterator
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    TypeVar,
)
from unittest import mock
from unittest.mock import MagicMock

from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.state.logger.logger_proxy import RealLoggerObject
from faststream._internal.subscriber.utils import MultiLock
from faststream._internal.testing.app import TestApp
from faststream._internal.testing.ast import is_contains_context_name
from faststream._internal.utils.functions import sync_fake_context

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.subscriber.proto import SubscriberProto


Broker = TypeVar("Broker", bound=BrokerUsecase[Any, Any])


class TestBroker(Generic[Broker]):
    """A class to represent a test broker."""

    # This is set so pytest ignores this class
    __test__ = False

    def __init__(
        self,
        broker: Broker,
        with_real: bool = False,
        connect_only: Optional[bool] = None,
    ) -> None:
        self.with_real = with_real
        self.broker = broker

        if connect_only is None:
            try:
                connect_only = is_contains_context_name(
                    self.__class__.__name__,
                    TestApp.__name__,
                )
            except Exception:  # pragma: no cover
                warnings.warn(
                    (
                        "\nError `{e!r}` occurred at `{self.__class__.__name__}` AST parsing."
                        "\n`connect_only` is set to `False` by default."
                    ),
                    category=RuntimeWarning,
                    stacklevel=1,
                )

                connect_only = False

        self.connect_only = connect_only
        self._fake_subscribers: list[SubscriberProto[Any]] = []

    async def __aenter__(self) -> Broker:
        self._ctx = self._create_ctx()
        return await self._ctx.__aenter__()

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await self._ctx.__aexit__(exc_type, exc_val, exc_tb)

    @asynccontextmanager
    async def _create_ctx(self) -> AsyncGenerator[Broker, None]:
        if self.with_real:
            self._fake_start(self.broker)
            context = sync_fake_context()
        else:
            context = self._patch_broker(self.broker)

        with context:
            async with self.broker:
                try:
                    if not self.connect_only:
                        await self.broker.start()
                    yield self.broker
                finally:
                    self._fake_close(self.broker)

    @contextmanager
    def _patch_producer(self, broker: Broker) -> Iterator[None]:
        raise NotImplementedError

    @contextmanager
    def _patch_logger(self, broker: Broker) -> Iterator[None]:
        state = broker._state.get()
        state._setup_logger_state()

        logger_state = state.logger_state
        old_log_object = logger_state.logger

        logger_state.logger = RealLoggerObject(MagicMock())
        try:
            yield
        finally:
            logger_state.logger = old_log_object

    @contextmanager
    def _patch_broker(self, broker: Broker) -> Generator[None, None, None]:
        with (
            mock.patch.object(
                broker,
                "start",
                wraps=partial(self._fake_start, broker),
            ),
            mock.patch.object(
                broker,
                "_connect",
                wraps=partial(self._fake_connect, broker),
            ),
            mock.patch.object(
                broker,
                "close",
            ),
            mock.patch.object(
                broker,
                "_connection",
                new=None,
            ),
            self._patch_producer(broker),
            self._patch_logger(broker),
            mock.patch.object(
                broker,
                "ping",
                return_value=True,
            ),
        ):
            broker._setup()
            yield

    def _fake_start(self, broker: Broker, *args: Any, **kwargs: Any) -> None:
        patch_broker_calls(broker)

        for p in broker._publishers:
            if getattr(p, "_fake_handler", None):
                continue

            sub, is_real = self.create_publisher_fake_subscriber(broker, p)

            if not is_real:
                self._fake_subscribers.append(sub)

            if not sub.calls:

                @sub
                async def publisher_response_subscriber(msg: Any) -> None:
                    pass

                broker.setup_subscriber(sub)

            if is_real:
                mock = MagicMock()
                p.set_test(mock=mock, with_fake=False)  # type: ignore[attr-defined]
                for h in sub.calls:
                    h.handler.set_test()
                    assert h.handler.mock  # nosec B101
                    h.handler.mock.side_effect = mock

            else:
                handler = sub.calls[0].handler
                handler.set_test()
                assert handler.mock  # nosec B101
                p.set_test(mock=handler.mock, with_fake=True)  # type: ignore[attr-defined]

        for subscriber in broker._subscribers:
            subscriber.running = True
            subscriber.lock = MultiLock()  # type: ignore[attr-defined]

    def _fake_close(
        self,
        broker: Broker,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        for p in broker._publishers:
            if getattr(p, "_fake_handler", None):
                p.reset_test()  # type: ignore[attr-defined]

        self.broker._subscribers = [
            sub for sub in self.broker._subscribers if sub not in self._fake_subscribers
        ]
        self._fake_subscribers.clear()

        for h in broker._subscribers:
            h.running = False
            for call in h.calls:
                call.handler.reset_test()

    @staticmethod
    @abstractmethod
    def create_publisher_fake_subscriber(
        broker: Broker,
        publisher: Any,
    ) -> tuple["SubscriberProto[Any]", bool]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    async def _fake_connect(broker: Broker, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError


def patch_broker_calls(broker: "BrokerUsecase[Any, Any]") -> None:
    """Patch broker calls."""
    broker._setup()

    for handler in broker._subscribers:
        for h in handler.calls:
            h.handler.set_test()
