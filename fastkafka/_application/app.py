# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/015_FastKafka.ipynb.

# %% auto 0
__all__ = ['logger', 'FastKafka', 'AwaitedMock']

# %% ../../nbs/015_FastKafka.ipynb 1
import asyncio
import functools
import inspect
import json
import types
from asyncio import iscoroutinefunction  # do not use the version from inspect
from collections import namedtuple
from contextlib import AbstractAsyncContextManager
from datetime import datetime, timedelta
from functools import wraps
from inspect import signature
from pathlib import Path
from typing import *
from unittest.mock import AsyncMock, MagicMock

import anyio
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

import fastkafka._components.logger

fastkafka._components.logger.should_supress_timestamps = True

import fastkafka
from fastkafka._components.aiokafka_consumer_loop import (
    aiokafka_consumer_loop,
    sanitize_kafka_config,
)
from .._components.aiokafka_producer_manager import AIOKafkaProducerManager
from fastkafka._components.asyncapi import (
    ConsumeCallable,
    ContactInfo,
    KafkaBroker,
    KafkaBrokers,
    KafkaServiceInfo,
    export_async_spec,
)
from .._components.benchmarking import _benchmark
from .._components.logger import get_logger
from .._components.meta import delegates, export, filter_using_signature, patch
from .._components.producer_decorator import ProduceCallable, producer_decorator

# %% ../../nbs/015_FastKafka.ipynb 3
logger = get_logger(__name__)

# %% ../../nbs/015_FastKafka.ipynb 9
@delegates(AIOKafkaConsumer, but=["bootstrap_servers"])
@delegates(AIOKafkaProducer, but=["bootstrap_servers"], keep=True)
def _get_kafka_config(
    **kwargs: Any,
) -> Dict[str, Any]:
    """Get kafka config"""
    allowed_keys = set(signature(_get_kafka_config).parameters.keys())
    if not set(kwargs.keys()) <= allowed_keys:
        unallowed_keys = ", ".join(
            sorted([f"'{x}'" for x in set(kwargs.keys()).difference(allowed_keys)])
        )
        raise ValueError(f"Unallowed key arguments passed: {unallowed_keys}")
    retval = kwargs.copy()

    # todo: check this values
    config_defaults = {
        "bootstrap_servers": "localhost:9092",
        "auto_offset_reset": "earliest",
        "max_poll_records": 100,
        #         "max_buffer_size": 10_000,
    }
    for key, value in config_defaults.items():
        if key not in retval:
            retval[key] = value

    return retval

# %% ../../nbs/015_FastKafka.ipynb 12
def _get_kafka_brokers(kafka_brokers: Optional[Dict[str, Any]] = None) -> KafkaBrokers:
    """Get Kafka brokers

    Args:
        kafka_brokers: Kafka brokers

    """
    if kafka_brokers is None:
        retval: KafkaBrokers = KafkaBrokers(
            brokers={
                "localhost": KafkaBroker(
                    url="https://localhost",
                    description="Local (dev) Kafka broker",
                    port="9092",
                )
            }
        )
    else:
        retval = KafkaBrokers(
            brokers={
                k: KafkaBroker.parse_raw(
                    v.json() if hasattr(v, "json") else json.dumps(v)
                )
                for k, v in kafka_brokers.items()
            }
        )

    return retval

# %% ../../nbs/015_FastKafka.ipynb 14
def _get_topic_name(
    topic_callable: Union[ConsumeCallable, ProduceCallable], prefix: str = "on_"
) -> str:
    """Get topic name
    Args:
        topic_callable: a function
        prefix: prefix of the name of the function followed by the topic name

    Returns:
        The name of the topic
    """
    topic = topic_callable.__name__
    if not topic.startswith(prefix) or len(topic) <= len(prefix):
        raise ValueError(f"Function name '{topic}' must start with {prefix}")
    topic = topic[len(prefix) :]

    return topic

# %% ../../nbs/015_FastKafka.ipynb 16
def _get_contact_info(
    name: str = "Author",
    url: str = "https://www.google.com",
    email: str = "noreply@gmail.com",
) -> ContactInfo:
    return ContactInfo(name=name, url=url, email=email)

# %% ../../nbs/015_FastKafka.ipynb 18
I = TypeVar("I", bound=BaseModel)
O = TypeVar("O", BaseModel, Awaitable[BaseModel])

F = TypeVar("F", bound=Callable)

# %% ../../nbs/015_FastKafka.ipynb 19
@export("fastkafka")
class FastKafka:
    @delegates(_get_kafka_config)
    def __init__(
        self,
        *,
        title: Optional[str] = None,
        description: Optional[str] = None,
        version: Optional[str] = None,
        contact: Optional[Dict[str, str]] = None,
        kafka_brokers: Dict[str, Any],
        root_path: Optional[Union[Path, str]] = None,
        lifespan: Optional[Callable[["FastKafka"], AsyncContextManager[None]]] = None,
        **kwargs: Any,
    ):
        """Creates FastKafka application

        Args:
            title: optional title for the documentation. If None,
                the title will be set to empty string
            description: optional description for the documentation. If
                None, the description will be set to empty string
            version: optional version for the documentation. If None,
                the version will be set to empty string
            contact: optional contact for the documentation. If None, the
                contact will be set to placeholder values:
                name='Author' url=HttpUrl('https://www.google.com', ) email='noreply@gmail.com'
            kafka_brokers: dictionary describing kafka brokers used for
                generating documentation
            root_path: path to where documentation will be created
            lifespan: asynccontextmanager that is used for setting lifespan hooks.
                __aenter__ is called before app start and __aexit__ after app stop.
                The lifespan is called whe application is started as async context
                manager, e.g.:`async with kafka_app...`

        """

        # this is needed for documentation generation
        self._title = title if title is not None else ""
        self._description = description if description is not None else ""
        self._version = version if version is not None else ""
        if contact is not None:
            self._contact_info = _get_contact_info(**contact)
        else:
            self._contact_info = _get_contact_info()

        self._kafka_service_info = KafkaServiceInfo(
            title=self._title,
            version=self._version,
            description=self._description,
            contact=self._contact_info,
        )
        self._kafka_brokers = _get_kafka_brokers(kafka_brokers)

        self._root_path = Path(".") if root_path is None else Path(root_path)

        self._asyncapi_path = self._root_path / "asyncapi"
        (self._asyncapi_path / "docs").mkdir(exist_ok=True, parents=True)
        (self._asyncapi_path / "spec").mkdir(exist_ok=True, parents=True)

        # this is used as default parameters for creating AIOProducer and AIOConsumer objects
        self._kafka_config = _get_kafka_config(**kwargs)

        #
        self._consumers_store: Dict[str, Tuple[ConsumeCallable, Dict[str, Any]]] = {}

        self._producers_store: Dict[  # type: ignore
            str, Tuple[ProduceCallable, AIOKafkaProducer, Dict[str, Any]]
        ] = {}

        self._producers_list: List[  # type: ignore
            Union[AIOKafkaProducer, AIOKafkaProducerManager]
        ] = []

        self.benchmark_results: Dict[str, Dict[str, Any]] = {}

        # background tasks
        self._scheduled_bg_tasks: List[Callable[..., Coroutine[Any, Any, Any]]] = []
        self._bg_task_group_generator: Optional[anyio.abc.TaskGroup] = None
        self._bg_tasks_group: Optional[anyio.abc.TaskGroup] = None

        # todo: use this for errrors
        self._on_error_topic: Optional[str] = None

        self.lifespan = lifespan
        self.lifespan_ctx: Optional[AsyncContextManager[None]] = None

        self._is_started: bool = False
        self._is_shutting_down: bool = False
        self._kafka_consumer_tasks: List[asyncio.Task[Any]] = []
        self._kafka_producer_tasks: List[asyncio.Task[Any]] = []
        self._running_bg_tasks: List[asyncio.Task[Any]] = []
        self.run = False

        # testing functions
        self.AppMocks = None
        self.mocks = None
        self.awaited_mocks = None

    @property
    def is_started(self) -> bool:
        return self._is_started

    def _set_bootstrap_servers(self, bootstrap_servers: str) -> None:
        self._kafka_config["bootstrap_servers"] = bootstrap_servers

    def set_kafka_broker(self, kafka_broker_name: str) -> None:
        if kafka_broker_name not in self._kafka_brokers.brokers:
            raise ValueError(
                f"Given kafka_broker_name '{kafka_broker_name}' is not found in kafka_brokers, available options are {self._kafka_brokers.brokers.keys()}"
            )

        broker_to_use = self._kafka_brokers.brokers[kafka_broker_name]
        bootstrap_servers = f"{broker_to_use.url}:{broker_to_use.port}"
        logger.info(
            f"set_kafka_broker() : Setting bootstrap_servers value to '{bootstrap_servers}'"
        )
        self._set_bootstrap_servers(bootstrap_servers=bootstrap_servers)

    async def __aenter__(self) -> "FastKafka":
        if self.lifespan is not None:
            self.lifespan_ctx = self.lifespan(self)
            await self.lifespan_ctx.__aenter__()
        await self._start()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[types.TracebackType],
    ) -> None:
        await self._stop()
        if self.lifespan_ctx is not None:
            await self.lifespan_ctx.__aexit__(exc_type, exc, tb)

    async def _start(self) -> None:
        raise NotImplementedError

    async def _stop(self) -> None:
        raise NotImplementedError

    def consumes(
        self,
        topic: Optional[str] = None,
        *,
        prefix: str = "on_",
        **kwargs: Dict[str, Any],
    ) -> ConsumeCallable:
        raise NotImplementedError

    def produces(  # type: ignore
        self,
        topic: Optional[str] = None,
        *,
        prefix: str = "to_",
        producer: Optional[AIOKafkaProducer] = None,
        **kwargs: Dict[str, Any],
    ) -> ProduceCallable:
        raise NotImplementedError

    def benchmark(
        self,
        interval: Union[int, timedelta] = 1,
        *,
        sliding_window_size: Optional[int] = None,
    ) -> Callable[[F], F]:
        raise NotImplementedError

    def run_in_background(
        self,
    ) -> Callable[[], Any]:
        raise NotImplementedError

    def _populate_consumers(
        self,
        is_shutting_down_f: Callable[[], bool],
    ) -> None:
        raise NotImplementedError

    def get_topics(self) -> Iterable[str]:
        raise NotImplementedError

    async def _populate_producers(self) -> None:
        raise NotImplementedError

    async def _populate_bg_tasks(self) -> None:
        raise NotImplementedError

    def create_docs(self) -> None:
        raise NotImplementedError

    def create_mocks(self) -> None:
        raise NotImplementedError

    async def _shutdown_consumers(self) -> None:
        raise NotImplementedError

    async def _shutdown_producers(self) -> None:
        raise NotImplementedError

    async def _shutdown_bg_tasks(self) -> None:
        raise NotImplementedError

# %% ../../nbs/015_FastKafka.ipynb 25
@patch
@delegates(AIOKafkaConsumer)
def consumes(
    self: FastKafka,
    topic: Optional[str] = None,
    *,
    prefix: str = "on_",
    **kwargs: Dict[str, Any],
) -> Callable[[ConsumeCallable], ConsumeCallable]:
    """Decorator registering the callback called when a message is received in a topic.

    This function decorator is also responsible for registering topics for AsyncAPI specificiation and documentation.

    Args:
        topic: Kafka topic that the consumer will subscribe to and execute the
            decorated function when it receives a message from the topic,
            default: None. If the topic is not specified, topic name will be
            inferred from the decorated function name by stripping the defined prefix
        prefix: Prefix stripped from the decorated function to define a topic name
            if the topic argument is not passed, default: "on_". If the decorated
            function name is not prefixed with the defined prefix and topic argument
            is not passed, then this method will throw ValueError

    Returns:
        A function returning the same function

    Throws:
        ValueError

    """

    def _decorator(
        on_topic: ConsumeCallable,
        topic: Optional[str] = topic,
        kwargs: Dict[str, Any] = kwargs,
    ) -> ConsumeCallable:
        topic_resolved: str = (
            _get_topic_name(topic_callable=on_topic, prefix=prefix)
            if topic is None
            else topic
        )

        self._consumers_store[topic_resolved] = (on_topic, kwargs)

        return on_topic

    return _decorator

# %% ../../nbs/015_FastKafka.ipynb 27
@patch
@delegates(AIOKafkaProducer)
def produces(
    self: FastKafka,
    topic: Optional[str] = None,
    *,
    prefix: str = "to_",
    **kwargs: Dict[str, Any],
) -> Callable[[ProduceCallable], ProduceCallable]:
    """Decorator registering the callback called when delivery report for a produced message is received

    This function decorator is also responsible for registering topics for AsyncAPI specificiation and documentation.

    Args:
        topic: Kafka topic that the producer will send returned values from
            the decorated function to, default: None- If the topic is not
            specified, topic name will be inferred from the decorated function
            name by stripping the defined prefix.
        prefix: Prefix stripped from the decorated function to define a topic
            name if the topic argument is not passed, default: "to_". If the
            decorated function name is not prefixed with the defined prefix
            and topic argument is not passed, then this method will throw ValueError

    Returns:
        A function returning the same function

    Raises:
        ValueError: when needed
    """

    def _decorator(
        on_topic: ProduceCallable,
        topic: Optional[str] = topic,
        kwargs: Dict[str, Any] = kwargs,
    ) -> ProduceCallable:
        topic_resolved: str = (
            _get_topic_name(topic_callable=on_topic, prefix=prefix)
            if topic is None
            else topic
        )

        self._producers_store[topic_resolved] = (on_topic, None, kwargs)
        return producer_decorator(self._producers_store, on_topic, topic_resolved)

    return _decorator

# %% ../../nbs/015_FastKafka.ipynb 29
@patch
def get_topics(self: FastKafka) -> Iterable[str]:
    produce_topics = set(self._producers_store.keys())
    consume_topics = set(self._consumers_store.keys())
    return consume_topics.union(produce_topics)

# %% ../../nbs/015_FastKafka.ipynb 31
@patch
def run_in_background(
    self: FastKafka,
) -> Callable[
    [Callable[..., Coroutine[Any, Any, Any]]], Callable[..., Coroutine[Any, Any, Any]]
]:
    """
    Decorator to schedule a task to be run in the background.

    This decorator is used to schedule a task to be run in the background when the app's `_on_startup` event is triggered.

    Returns:
        Callable[None, None]: A decorator function that takes a background task as an input and stores it to be run in the backround.
    """

    def _decorator(
        bg_task: Callable[..., Coroutine[Any, Any, Any]]
    ) -> Callable[..., Coroutine[Any, Any, Any]]:
        """
        Store the background task.

        Args:
            bg_task (Callable[[], None]): The background task to be run asynchronously.

        Returns:
            Callable[[], None]: Original background task.
        """
        logger.info(
            f"run_in_background() : Adding function '{bg_task.__name__}' as background task"
        )
        self._scheduled_bg_tasks.append(bg_task)

        return bg_task

    return _decorator

# %% ../../nbs/015_FastKafka.ipynb 35
@patch
def _populate_consumers(
    self: FastKafka,
    is_shutting_down_f: Callable[[], bool],
) -> None:
    default_config: Dict[str, Any] = filter_using_signature(
        AIOKafkaConsumer, **self._kafka_config
    )
    self._kafka_consumer_tasks = [
        asyncio.create_task(
            aiokafka_consumer_loop(
                topic=topic,
                callback=consumer,
                msg_type=signature(consumer).parameters["msg"].annotation,
                is_shutting_down_f=is_shutting_down_f,
                **{**default_config, **override_config},
            )
        )
        for topic, (consumer, override_config) in self._consumers_store.items()
    ]


@patch
async def _shutdown_consumers(
    self: FastKafka,
) -> None:
    if self._kafka_consumer_tasks:
        await asyncio.wait(self._kafka_consumer_tasks)

# %% ../../nbs/015_FastKafka.ipynb 37
# TODO: Add passing of vars
async def _create_producer(  # type: ignore
    *,
    callback: ProduceCallable,
    default_config: Dict[str, Any],
    override_config: Dict[str, Any],
    producers_list: List[Union[AIOKafkaProducer, AIOKafkaProducerManager]],
) -> Union[AIOKafkaProducer, AIOKafkaProducerManager]:
    """Creates a producer

    Args:
        callback: A callback function that is called when the producer is ready.
        producer: An existing producer to use.
        default_config: A dictionary of default configuration values.
        override_config: A dictionary of configuration values to override.
        producers_list: A list of producers to add the new producer to.

    Returns:
        A producer.
    """

    config = {
        **filter_using_signature(AIOKafkaProducer, **default_config),
        **filter_using_signature(AIOKafkaProducer, **override_config),
    }
    producer = AIOKafkaProducer(**config)
    logger.info(
        f"_create_producer() : created producer using the config: '{sanitize_kafka_config(**config)}'"
    )

    await producer.start()

    producers_list.append(producer)

    return producer


@patch
async def _populate_producers(self: FastKafka) -> None:
    """Populates the producers for the FastKafka instance.

    Args:
        self: The FastKafka instance.

    Returns:
        None.

    Raises:
        None.
    """
    default_config: Dict[str, Any] = self._kafka_config
    self._producers_list = []
    self._producers_store.update(
        {
            topic: (
                callback,
                await _create_producer(
                    callback=callback,
                    default_config=default_config,
                    override_config=override_config,
                    producers_list=self._producers_list,
                ),
                override_config,
            )
            for topic, (
                callback,
                _,
                override_config,
            ) in self._producers_store.items()
        }
    )


@patch
async def _shutdown_producers(self: FastKafka) -> None:
    [await producer.stop() for producer in self._producers_list[::-1]]
    # Remove references to stale producers
    self._producers_list = []
    self._producers_store.update(
        {
            topic: (
                callback,
                None,
                override_config,
            )
            for topic, (
                callback,
                _,
                override_config,
            ) in self._producers_store.items()
        }
    )

# %% ../../nbs/015_FastKafka.ipynb 39
@patch
async def _populate_bg_tasks(
    self: FastKafka,
) -> None:
    def _start_bg_task(task: Callable[..., Coroutine[Any, Any, Any]]) -> asyncio.Task:
        logger.info(
            f"_populate_bg_tasks() : Starting background task '{task.__name__}'"
        )
        return asyncio.create_task(task(), name=task.__name__)

    self._running_bg_tasks = [_start_bg_task(task) for task in self._scheduled_bg_tasks]


@patch
async def _shutdown_bg_tasks(
    self: FastKafka,
) -> None:
    for task in self._running_bg_tasks:
        logger.info(
            f"_shutdown_bg_tasks() : Cancelling background task '{task.get_name()}'"
        )
        task.cancel()

    for task in self._running_bg_tasks:
        logger.info(
            f"_shutdown_bg_tasks() : Waiting for background task '{task.get_name()}' to finish"
        )
        try:
            await task
        except asyncio.CancelledError:
            pass
        logger.info(
            f"_shutdown_bg_tasks() : Execution finished for background task '{task.get_name()}'"
        )

# %% ../../nbs/015_FastKafka.ipynb 41
@patch
async def _start(self: FastKafka) -> None:
    def is_shutting_down_f(self: FastKafka = self) -> bool:
        return self._is_shutting_down

    #     self.create_docs()
    await self._populate_producers()
    self._populate_consumers(is_shutting_down_f)
    await self._populate_bg_tasks()

    self._is_started = True


@patch
async def _stop(self: FastKafka) -> None:
    self._is_shutting_down = True

    await self._shutdown_bg_tasks()
    await self._shutdown_consumers()
    await self._shutdown_producers()

    self._is_shutting_down = False
    self._is_started = False

# %% ../../nbs/015_FastKafka.ipynb 47
@patch
def create_docs(self: FastKafka) -> None:
    export_async_spec(
        consumers={
            topic: callback for topic, (callback, _) in self._consumers_store.items()
        },
        producers={
            topic: callback for topic, (callback, _, _) in self._producers_store.items()
        },
        kafka_brokers=self._kafka_brokers,
        kafka_service_info=self._kafka_service_info,
        asyncapi_path=self._asyncapi_path,
    )

# %% ../../nbs/015_FastKafka.ipynb 51
class AwaitedMock:
    @staticmethod
    def _await_for(f: Callable[..., Any]) -> Callable[..., Any]:
        @delegates(f)
        async def inner(
            *args: Any, f: Callable[..., Any] = f, timeout: int = 60, **kwargs: Any
        ) -> Any:
            if inspect.iscoroutinefunction(f):
                return await asyncio.wait_for(f(*args, **kwargs), timeout=timeout)
            else:
                t0 = datetime.now()
                e: Optional[Exception] = None
                while True:
                    try:
                        return f(*args, **kwargs)
                    except Exception as _e:
                        await asyncio.sleep(1)
                        e = _e

                    if datetime.now() - t0 > timedelta(seconds=timeout):
                        break

                raise e

        return inner

    def __init__(self, o: Any):
        self._o = o

        for name in o.__dir__():
            if not name.startswith("_"):
                f = getattr(o, name)
                if inspect.ismethod(f):
                    setattr(self, name, self._await_for(f))

# %% ../../nbs/015_FastKafka.ipynb 52
@patch
def create_mocks(self: FastKafka) -> None:
    """Creates self.mocks as a named tuple mapping a new function obtained by calling the original functions and a mock"""
    app_methods = [f for f, _ in self._consumers_store.values()] + [
        f for f, _, _ in self._producers_store.values()
    ]
    self.AppMocks = namedtuple(  # type: ignore
        f"{self.__class__.__name__}Mocks", [f.__name__ for f in app_methods]
    )

    self.mocks = self.AppMocks(  # type: ignore
        **{
            f.__name__: AsyncMock() if inspect.iscoroutinefunction(f) else MagicMock()
            for f in app_methods
        }
    )

    self.awaited_mocks = self.AppMocks(  # type: ignore
        **{name: AwaitedMock(mock) for name, mock in self.mocks._asdict().items()}
    )

    def add_mock(
        f: Callable[..., Any], mock: Union[AsyncMock, MagicMock]
    ) -> Callable[..., Any]:
        """Add call to mock when calling function f"""

        @functools.wraps(f)
        async def async_inner(
            *args: Any, f: Callable[..., Any] = f, mock: AsyncMock = mock, **kwargs: Any
        ) -> Any:
            await mock(*args, **kwargs)
            return await f(*args, **kwargs)

        @functools.wraps(f)
        def sync_inner(
            *args: Any, f: Callable[..., Any] = f, mock: MagicMock = mock, **kwargs: Any
        ) -> Any:
            mock(*args, **kwargs)
            return f(*args, **kwargs)

        if inspect.iscoroutinefunction(f):
            return async_inner
        else:
            return sync_inner

    self._consumers_store.update(
        {
            name: (
                add_mock(f, getattr(self.mocks, f.__name__)),
                kwargs,
            )
            for name, (f, kwargs) in self._consumers_store.items()
        }
    )

    self._producers_store.update(
        {
            name: (
                add_mock(f, getattr(self.mocks, f.__name__)),
                producer,
                kwargs,
            )
            for name, (f, producer, kwargs) in self._producers_store.items()
        }
    )

# %% ../../nbs/015_FastKafka.ipynb 58
@patch
def benchmark(
    self: FastKafka,
    interval: Union[int, timedelta] = 1,
    *,
    sliding_window_size: Optional[int] = None,
) -> Callable[[Callable[[I], Optional[O]]], Callable[[I], Optional[O]]]:
    """Decorator to benchmark produces/consumes functions

    Args:
        interval: Period to use to calculate throughput. If value is of type int,
            then it will be used as seconds. If value is of type timedelta,
            then it will be used as it is. default: 1 - one second
        sliding_window_size: The size of the sliding window to use to calculate
            average throughput. default: None - By default average throughput is
            not calculated
    """

    def _decorator(func: Callable[[I], Optional[O]]) -> Callable[[I], Optional[O]]:
        func_name = f"{func.__module__}.{func.__qualname__}"

        @wraps(func)
        def wrapper(
            *args: I,
            **kwargs: I,
        ) -> Optional[O]:
            _benchmark(
                interval=interval,
                sliding_window_size=sliding_window_size,
                func_name=func_name,
                benchmark_results=self.benchmark_results,
            )
            return func(*args, **kwargs)

        @wraps(func)
        async def async_wrapper(
            *args: I,
            **kwargs: I,
        ) -> Optional[O]:
            _benchmark(
                interval=interval,
                sliding_window_size=sliding_window_size,
                func_name=func_name,
                benchmark_results=self.benchmark_results,
            )
            return await func(*args, **kwargs)  # type: ignore

        if inspect.iscoroutinefunction(func):
            return async_wrapper  # type: ignore
        else:
            return wrapper

    return _decorator
