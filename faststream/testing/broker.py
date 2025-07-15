import warnings
from abc import abstractmethod
from contextlib import asynccontextmanager, contextmanager
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Generator,
    Generic,
    List,
    Optional,
    Tuple,
    Type,
    TypeVar,
)
from unittest import mock
from unittest.mock import MagicMock

from faststream.broker.core.usecase import BrokerUsecase
from faststream.testing.app import TestApp
from faststream.utils.ast import is_contains_context_name
from faststream.utils.functions import sync_fake_context

if TYPE_CHECKING:
    from types import TracebackType

    from faststream.broker.subscriber.proto import SubscriberProto


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
        self._fake_subscribers: List[SubscriberProto[Any]] = []

    async def __aenter__(self) -> Broker:
        self._ctx = self._create_ctx()
        return await self._ctx.__aenter__()

    async def __aexit__(self, *args: Any) -> None:
        await self._ctx.__aexit__(*args)

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
    def _patch_broker(self, broker: Broker) -> Generator[None, None, None]:
        with mock.patch.object(
            broker,
            "start",
            wraps=partial(self._fake_start, broker),
        ), mock.patch.object(
            broker,
            "_connect",
            wraps=partial(self._fake_connect, broker),
        ), mock.patch.object(
            broker,
            "stop",
        ), mock.patch.object(
            broker,
            "_connection",
            new=None,
        ), mock.patch.object(
            broker,
            "_producer",
            new=None,
        ), mock.patch.object(
            broker,
            "ping",
            return_value=True,
        ), mock.patch.object(
            broker,  # TODO: remove it in 0.7
            "close",  # patch stop to save compatibility
        ):
            yield

    def _fake_start(self, broker: Broker, *args: Any, **kwargs: Any) -> None:
        broker.setup()

        patch_broker_calls(broker)

        for p in broker._publishers.values():
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

        for subscriber in broker._subscribers.values():
            subscriber.running = True

    def _fake_close(
        self,
        broker: Broker,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        for p in broker._publishers.values():
            if getattr(p, "_fake_handler", None):
                p.reset_test()  # type: ignore[attr-defined]

        for sub in self._fake_subscribers:
            self.broker._subscribers.pop(hash(sub), None)  # type: ignore[attr-defined]
        self._fake_subscribers = []

        for h in broker._subscribers.values():
            h.running = False
            for call in h.calls:
                call.handler.reset_test()

    @staticmethod
    @abstractmethod
    def create_publisher_fake_subscriber(
        broker: Broker, publisher: Any
    ) -> Tuple["SubscriberProto[Any]", bool]:
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    async def _fake_connect(broker: Broker, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError


def patch_broker_calls(broker: "BrokerUsecase[Any, Any]") -> None:
    """Patch broker calls."""
    broker._abc_start()

    for handler in broker._subscribers.values():
        for h in handler.calls:
            h.handler.set_test()
