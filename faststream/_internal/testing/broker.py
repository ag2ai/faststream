import warnings
from abc import abstractmethod
from collections.abc import AsyncGenerator, Generator
from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
    contextmanager,
)
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    Protocol,
    TypeVar,
    cast,
)
from unittest import mock
from unittest.mock import MagicMock

from typing_extensions import TypeVar as TypeVar313

from faststream._internal.broker import BrokerUsecase
from faststream._internal.config_value import ConfigSource
from faststream._internal.logger.logger_proxy import RealLoggerObject
from faststream._internal.testing.app import TestApp
from faststream._internal.testing.ast import is_contains_context_name

if TYPE_CHECKING:
    from types import TracebackType

    from faststream._internal.endpoint.subscriber import SubscriberUsecase


Broker = TypeVar("Broker", bound=BrokerUsecase[Any, Any])

# ``__aenter__`` return type. Each concrete ``TestBroker`` subclass binds it to a
# single broker or a ``tuple`` of brokers via its overloaded ``__init__``.
EnterType = TypeVar313("EnterType", default=Any)


class _ProducerContains(Protocol):
    producer: Any


@contextmanager
def change_producer(
    config: _ProducerContains,
    producer: Any,
) -> Generator[None, None, None]:
    old_producer, config.producer = config.producer, producer
    yield
    config.producer = old_producer


class TestBroker(Generic[Broker, EnterType]):
    """A class to represent a test broker."""

    # This is set so pytest ignores this class
    __test__ = False

    def __init__(
        self,
        *brokers: Broker,
        with_real: bool = False,
        connect_only: bool | None = None,
        config_values: ConfigSource = None,
    ) -> None:
        self.with_real = with_real
        self.brokers = brokers
        self.config_values = config_values

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
        self._fake_subscribers: list[SubscriberUsecase[Any]] = []

    async def __aenter__(self) -> EnterType:
        self._ctx = self._create_ctx()
        brokers = await self._ctx.__aenter__()
        if len(brokers) == 1:
            return cast("EnterType", brokers[0])
        return cast("EnterType", brokers)

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await self._ctx.__aexit__(exc_type, exc_val, exc_tb)

    @asynccontextmanager
    async def _create_ctx(self) -> AsyncGenerator[list[Broker], None]:
        async with AsyncExitStack() as stack:
            saved_running = {}
            started_brokers = []

            # Two loops rather than one, because a Broker's fake-publisher scan
            # reads every Broker's Subscribers, not only its own. Interleaving
            # would have the first Broker scan Subscribers belonging to one the
            # loop has not connected yet, and an endpoint refuses a read taken
            # before its Preparation. The group is what a scan reads, so the
            # group is what has to be connected before any member scans it.
            for broker in self.brokers:
                stack.enter_context(self._patch_config_values(broker))

                if not self.with_real:
                    stack.enter_context(self._patch_broker(broker))

                await stack.enter_async_context(broker)

            for broker in self.brokers:
                # After the connection, because it reads a Publisher's
                # destination and Preparation is what resolves one. An in-memory
                # Broker reaches the same call through its patched `start`.
                if self.with_real:
                    self._fake_start(broker)

                for sub in broker.subscribers:
                    saved_running[sub] = sub.running

                started_brokers.append(
                    await stack.enter_async_context(self._do_start(broker))
                )

            yield started_brokers

            for sub, was_running in saved_running.items():
                sub.running = was_running

    @asynccontextmanager
    async def _do_start(self, broker: Broker) -> AsyncGenerator[Broker, None]:
        try:
            if not self.connect_only:
                await broker.start()

            yield broker

        finally:
            self._fake_close(broker)

    @contextmanager
    def _patch_config_values(self, broker: Broker) -> Generator[None, None, None]:
        """Make the test broker's Config values beat the Broker's own.

        They are a level of their own, above the Broker and the App, so a Broker
        whose values live on an App it is not part of here is still testable.
        """
        composition = broker.config
        saved, composition.config_values_override = (
            composition.config_values_override,
            self.config_values,
        )
        # Both edges, because both are a change of the values in scope. Entering
        # is as much a change as leaving: a Broker already prepared -- by an
        # earlier context, or by an AsyncAPI render -- would otherwise keep the
        # addresses derived against the values this context just replaced.
        broker._invalidate()

        try:
            yield

        finally:
            composition.config_values_override = saved
            broker._invalidate()

    @contextmanager
    def _patch_producer(self, broker: Broker) -> Generator[None, None, None]:
        raise NotImplementedError

    @contextmanager
    def _patch_logger(self, broker: Broker) -> Generator[None, None, None]:
        # No `_setup_logger` here: it reads every Subscriber's log context,
        # which reads their resolved addresses, and this runs before the
        # connection that resolves them. Preparation performs that step itself,
        # and finds the mock already in place — it builds a logger only where
        # there is not one.
        logger_state = broker.config.logger

        old_log_object, logger_state.logger = (
            logger_state.logger,
            RealLoggerObject(MagicMock()),
        )

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
                "stop",
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
            yield

    def _fake_start(self, broker: Broker, *args: Any, **kwargs: Any) -> None:
        # Everything below reads an endpoint's resolved addresses, and
        # Preparation is what settles one. A Broker reaches here prepared by the
        # connection it just made, except where the connection was established
        # before this context replaced its Config values — the Redis cluster
        # path connects ahead of that — and the invalidation which followed has
        # no `connect()` after it to prepare again. Idempotent, so the ordinary
        # case is a no-op.
        broker._prepare()

        for publisher in broker.publishers:
            if getattr(publisher, "_fake_handler", None):
                continue

            sub, is_real = self.create_publisher_fake_subscriber(broker, publisher)

            if not is_real:
                self._fake_subscribers.append(sub)

            if not sub.calls:

                @sub
                async def publisher_response_subscriber(msg: Any) -> None:
                    pass

            # As soon as its handler is attached, rather than once the loop is
            # done: the next Publisher scans every Subscriber for a match, and
            # reads the addresses of the fake one just created.
            sub.prepare()

            if is_real:
                mock = MagicMock()
                publisher.set_test(mock=mock, with_fake=False)
                for h in sub.calls:
                    h.handler.set_test()
                    assert h.handler.mock
                    h.handler.mock.side_effect = mock

            else:
                handler = sub.calls[0].handler
                handler.set_test()
                assert handler.mock
                publisher.set_test(mock=handler.mock, with_fake=True)

        patch_broker_calls(broker)

        for subscriber in broker.subscribers:
            subscriber._post_start()

    def _fake_close(
        self,
        broker: Broker,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        for p in broker.publishers:
            if getattr(p, "_fake_handler", None):
                p.reset_test()

        self._fake_subscribers.clear()

        for sub in broker.subscribers:
            sub.running = False
            for call in sub.calls:
                call.handler.reset_test()

    @abstractmethod
    def create_publisher_fake_subscriber(
        self,
        broker: Broker,
        publisher: Any,
    ) -> tuple["SubscriberUsecase[Any]", bool]:
        raise NotImplementedError

    @abstractmethod
    async def _fake_connect(self, broker: Broker, *args: Any, **kwargs: Any) -> None:
        raise NotImplementedError


def patch_broker_calls(broker: "BrokerUsecase[Any, Any]") -> None:
    """Patch broker calls."""
    for sub in broker.subscribers:
        sub._build_fastdepends_model()

        for h in sub.calls:
            h.handler.set_test()
