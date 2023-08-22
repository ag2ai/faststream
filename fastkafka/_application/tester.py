# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/016_Tester.ipynb.

# %% auto 0
__all__ = ['Tester', 'mirror_producer', 'mirror_consumer', 'AmbiguousWarning', 'set_sugar']

# %% ../../nbs/016_Tester.ipynb 1
import asyncio
import collections
import inspect
from unittest.mock import AsyncMock, MagicMock
import json
from contextlib import asynccontextmanager
from itertools import groupby
from typing import *
from types import ModuleType

from pydantic import BaseModel

from .. import KafkaEvent
from .app import FastKafka, AwaitedMock, _get_kafka_brokers
from .._components.asyncapi import KafkaBroker, KafkaBrokers
from .._components.helpers import unwrap_list_type
from .._components.meta import delegates, export, patch
from .._components.producer_decorator import unwrap_from_kafka_event
from .._components.aiokafka_consumer_loop import ConsumeCallable
from .._testing.apache_kafka_broker import ApacheKafkaBroker
from .._testing.in_memory_broker import InMemoryBroker
from .._testing.local_redpanda_broker import LocalRedpandaBroker
from .._components.helpers import remove_suffix

# %% ../../nbs/016_Tester.ipynb 7
def _get_broker_spec(bootstrap_server: str) -> KafkaBroker:
    """
    Helper function to get the broker specification from the bootstrap server URL.

    Args:
        bootstrap_server: The bootstrap server URL in the format "<host>:<port>".

    Returns:
        A KafkaBroker object representing the broker specification.
    """
    url = bootstrap_server.split(":")[0]
    port = bootstrap_server.split(":")[1]
    return KafkaBroker(url=url, port=port, description="", protocol="")

# %% ../../nbs/016_Tester.ipynb 9
@export("fastkafka.testing")
class Tester(FastKafka):
    __test__ = False

    def __init__(
        self,
        app: Union[FastKafka, List[FastKafka]],
        *,
        use_in_memory_broker: bool = True,
    ):
        """Mirror-like object for testing a FastKafka application

        Can be used as context manager

        Args:
            app: The FastKafka application to be tested.
            use_in_memory_broker: Whether to use an in-memory broker for testing or not.
        """
        self.apps = app if isinstance(app, list) else [app]

        for app in self.apps:
            app.create_mocks()

        super().__init__()
        self.mirrors: Dict[Any, Any] = {}
        self._kafka_brokers = self.apps[0]._kafka_brokers
        self._kafka_config["bootstrap_servers_id"] = self.apps[0]._kafka_config[
            "bootstrap_servers_id"
        ]
        self._create_mirrors()
        self.use_in_memory_broker = use_in_memory_broker

    async def _start_tester(self) -> None:
        """Starts the Tester"""
        for app in self.apps:
            await app.__aenter__()
        self.create_mocks()
        self._arrange_mirrors()
        await super().__aenter__()
        await asyncio.sleep(3)

    async def _stop_tester(self) -> None:
        """Shuts down the Tester"""
        await super().__aexit__(None, None, None)
        for app in self.apps[::-1]:
            await app.__aexit__(None, None, None)

    def _create_mirrors(self) -> None:
        pass

    def _arrange_mirrors(self) -> None:
        pass

    def _set_arguments_and_return_old(
        self, bootstrap_servers_id: Optional[str], use_in_memory_broker: bool
    ) -> Dict[Any, Any]:
        initial_arguments: Dict[Any, Any] = dict()
        initial_arguments["use_in_memory_broker"] = self.use_in_memory_broker
        self.use_in_memory_broker = use_in_memory_broker

        initial_arguments["bootstrap_servers_id"] = self._kafka_config[
            "bootstrap_servers_id"
        ]
        if bootstrap_servers_id is None:
            bootstrap_servers_id = self._kafka_config["bootstrap_servers_id"]
        else:
            self._kafka_config["bootstrap_servers_id"] = bootstrap_servers_id

        for app in self.apps:
            initial_arguments[app] = app._kafka_config["bootstrap_servers_id"]
            app._kafka_config["bootstrap_servers_id"] = bootstrap_servers_id

        return initial_arguments

    def _restore_initial_arguments(self, initial_arguments: Dict[Any, Any]) -> None:
        self.use_in_memory_broker = initial_arguments["use_in_memory_broker"]
        self._kafka_config["bootstrap_servers_id"] = initial_arguments[
            "bootstrap_servers_id"
        ]

        for app in self.apps:
            app._kafka_config["bootstrap_servers_id"] = initial_arguments[app]

    @asynccontextmanager
    async def using_external_broker(
        self,
        bootstrap_servers_id: Optional[str] = None,
    ) -> AsyncGenerator["Tester", None]:
        """Tester context manager for using external broker

        Args:
            bootstrap_servers_id: The bootstrap server of aplications.

        Returns:
            self or None
        """
        initial_arguments = self._set_arguments_and_return_old(
            bootstrap_servers_id, use_in_memory_broker=False
        )

        async with self._create_ctx() as ctx:
            try:
                yield self
            finally:
                self._restore_initial_arguments(initial_arguments)

    @asynccontextmanager
    async def using_inmemory_broker(
        self,
        bootstrap_servers_id: Optional[str] = None,
    ) -> AsyncGenerator["Tester", None]:
        """Tester context manager for using in-memory broker

        Args:
            bootstrap_servers_id: The bootstrap server of aplications.

        Returns:
            self or None
        """
        initial_arguments = self._set_arguments_and_return_old(
            bootstrap_servers_id, use_in_memory_broker=True
        )

        async with self._create_ctx() as ctx:
            try:
                yield self
            finally:
                self._restore_initial_arguments(initial_arguments)

    @asynccontextmanager
    async def _create_ctx(self) -> AsyncGenerator["Tester", None]:
        if self.use_in_memory_broker == True:
            with InMemoryBroker():  # type: ignore
                await self._start_tester()
                try:
                    yield self
                finally:
                    await self._stop_tester()
        else:
            await self._start_tester()
            try:
                yield self
            finally:
                await self._stop_tester()

    async def __aenter__(self) -> "Tester":
        self._ctx = self._create_ctx()
        return await self._ctx.__aenter__()

    async def __aexit__(self, *args: Any) -> None:
        await self._ctx.__aexit__(*args)

# %% ../../nbs/016_Tester.ipynb 16
def mirror_producer(
    topic: str, producer_f: Callable[..., Any], brokers: str, app: FastKafka
) -> Callable[..., Any]:
    """
    Decorator to create a mirrored producer function.

    Args:
        topic: The topic to produce to.
        producer_f: The original producer function.
        brokers: The brokers configuration.
        app: The FastKafka application.

    Returns:
        The mirrored producer function.
    """
    msg_type = inspect.signature(producer_f).return_annotation

    msg_type_unwrapped = unwrap_list_type(unwrap_from_kafka_event(msg_type))

    async def skeleton_func(msg: BaseModel) -> None:
        pass

    mirror_func = skeleton_func
    sig = inspect.signature(skeleton_func)

    # adjust name, take into consideration the origin app and brokers
    # configuration so that we can differentiate those two
    mirror_func.__name__ = f"mirror_{id(app)}_on_{remove_suffix(topic).replace('.', '_').replace('-', '_')}_{abs(hash(brokers))}"

    # adjust arg and return val
    sig = sig.replace(
        parameters=[
            inspect.Parameter(
                name="msg",
                annotation=msg_type_unwrapped,
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
    )

    mirror_func.__signature__ = sig  # type: ignore

    return mirror_func

# %% ../../nbs/016_Tester.ipynb 19
def mirror_consumer(
    topic: str, consumer_f: Callable[..., Any], brokers: str, app: FastKafka
) -> Callable[[BaseModel], Coroutine[Any, Any, BaseModel]]:
    """
    Decorator to create a mirrored consumer function.

    Args:
        topic: The topic to consume from.
        consumer_f: The original consumer function.
        brokers: The brokers configuration.
        app: The FastKafka application.

    Returns:
        The mirrored consumer function.
    """
    msg_type = inspect.signature(consumer_f).parameters["msg"]

    msg_type_unwrapped = unwrap_list_type(msg_type)

    async def skeleton_func(msg: BaseModel) -> BaseModel:
        return msg

    mirror_func = skeleton_func
    sig = inspect.signature(skeleton_func)

    # adjust name, take into consideration the origin app and brokers
    # configuration so that we can differentiate those two
    mirror_func.__name__ = f"mirror_{id(app)}_to_{remove_suffix(topic).replace('.', '_').replace('-', '_')}_{abs(hash(brokers))}"

    # adjust arg and return val
    sig = sig.replace(
        parameters=[msg_type], return_annotation=msg_type_unwrapped.annotation
    )

    mirror_func.__signature__ = sig  # type: ignore
    return mirror_func

# %% ../../nbs/016_Tester.ipynb 21
@patch
def _create_mirrors(self: Tester) -> None:
    """
    Creates mirror functions for producers and consumers.

    Iterates over the FastKafka application and its producers and consumers. For each consumer, it creates a mirror
    consumer function using the `mirror_consumer` decorator. For each producer, it creates a mirror producer function
    using the `mirror_producer` decorator. The mirror functions are stored in the `self.mirrors` dictionary and also
    set as attributes on the Tester instance.

    Returns:
        None
    """
    for app in self.apps:
        for topic, (consumer_f, _, _, brokers, _) in app._consumers_store.items():
            mirror_f = mirror_consumer(
                topic,
                consumer_f,
                brokers.model_dump_json()
                if brokers is not None
                else app._kafka_brokers.model_dump_json(),
                app,
            )
            mirror_f = self.produces(  # type: ignore
                topic=remove_suffix(topic),
                brokers=brokers,
            )(mirror_f)
            self.mirrors[consumer_f] = mirror_f
            setattr(self, mirror_f.__name__, mirror_f)
        for topic, (producer_f, _, brokers, _) in app._producers_store.items():
            mirror_f = mirror_producer(
                topic,
                producer_f,
                brokers.model_dump_json()
                if brokers is not None
                else app._kafka_brokers.model_dump_json(),
                app,
            )
            mirror_f = self.consumes(
                topic=remove_suffix(topic),
                brokers=brokers,
            )(
                mirror_f  # type: ignore
            )
            self.mirrors[producer_f] = mirror_f
            setattr(self, mirror_f.__name__, mirror_f)

# %% ../../nbs/016_Tester.ipynb 25
class AmbiguousWarning:
    """
    Warning class used for ambiguous topics.

    Args:
        topic: The ambiguous topic.
        functions: List of function names associated with the ambiguous topic.
    """

    def __init__(self, topic: str, functions: List[str]):
        self.topic = topic
        self.functions = functions

    def __getattribute__(self, attr: str) -> Any:
        raise RuntimeError(
            f"Ambiguous topic: {super().__getattribute__('topic')}, for functions: {super().__getattribute__('functions')}\nUse Tester.mirrors[app.function] to resolve ambiguity"
        )

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        raise RuntimeError(
            f"Ambiguous topic: {self.topic}, for functions: {self.functions}\nUse Tester.mirrors[app.function] to resolve ambiguity"
        )

# %% ../../nbs/016_Tester.ipynb 27
def set_sugar(
    *,
    tester: Tester,
    prefix: str,
    topic_brokers: Dict[str, Tuple[List[str], List[str]]],
    topic: str,
    brokers: str,
    origin_function_name: str,
    function: Callable[..., Union[Any, Awaitable[Any]]],
) -> None:
    """
    Sets the sugar function for a topic.

    Args:
        tester: The Tester instance.
        prefix: The prefix to use for the sugar function (e.g., "to_" or "on_").
        topic_brokers: Dictionary to store the brokers and functions associated with each topic.
        topic: The topic name.
        brokers: The brokers configuration.
        origin_function_name: The name of the original function.
        function: The mirror function to be set as the sugar function.

    Returns:
        None
    """
    brokers_for_topic, functions_for_topic = topic_brokers.get(topic, ([], []))
    if brokers not in brokers_for_topic:
        brokers_for_topic.append(brokers)
        functions_for_topic.append(origin_function_name)
        topic_brokers[topic] = (brokers_for_topic, functions_for_topic)
    if len(brokers_for_topic) == 1:
        setattr(tester, f"{prefix}{topic}", function)
    else:
        setattr(
            tester, f"{prefix}{topic}", AmbiguousWarning(topic, functions_for_topic)
        )

# %% ../../nbs/016_Tester.ipynb 28
@patch
def _arrange_mirrors(self: Tester) -> None:
    """
    Arranges the mirror functions.

    Iterates over the FastKafka application and its producers and consumers. For each consumer, it retrieves the mirror
    function from the `self.mirrors` dictionary and sets it as an attribute on the Tester instance. It also sets the
    sugar function using the `set_sugar` function. For each producer, it retrieves the mirror function and sets it as
    an attribute on the Tester instance. It also sets the sugar function for the awaited mocks. Finally, it creates the
    `mocks` and `awaited_mocks` namedtuples and sets them as attributes on the Tester instance.

    Returns:
        None
    """
    topic_brokers: Dict[str, Tuple[List[str], List[str]]] = {}
    mocks = {}
    awaited_mocks = {}
    for app in self.apps:
        for topic, (consumer_f, _, _, brokers, _) in app._consumers_store.items():
            mirror_f = self.mirrors[consumer_f]
            self.mirrors[getattr(app, consumer_f.__name__)] = mirror_f
            set_sugar(
                tester=self,
                prefix="to_",
                topic_brokers=topic_brokers,
                topic=remove_suffix(topic).replace(".", "_").replace("-", "_"),
                brokers=brokers.model_dump_json()
                if brokers is not None
                else app._kafka_brokers.model_dump_json(),
                origin_function_name=consumer_f.__name__,
                function=mirror_f,
            )

            mocks[
                f"to_{remove_suffix(topic).replace('.', '_').replace('-', '_')}"
            ] = getattr(self.mocks, mirror_f.__name__)
            awaited_mocks[
                f"to_{remove_suffix(topic).replace('.', '_').replace('-', '_')}"
            ] = getattr(self.awaited_mocks, mirror_f.__name__)

        for topic, (producer_f, _, brokers, _) in app._producers_store.items():
            mirror_f = self.mirrors[producer_f]
            self.mirrors[getattr(app, producer_f.__name__)] = getattr(
                self.awaited_mocks, mirror_f.__name__
            )
            set_sugar(
                tester=self,
                prefix="on_",
                topic_brokers=topic_brokers,
                topic=remove_suffix(topic).replace(".", "_").replace("-", "_"),
                brokers=brokers.model_dump_json()
                if brokers is not None
                else app._kafka_brokers.model_dump_json(),
                origin_function_name=producer_f.__name__,
                function=getattr(self.awaited_mocks, mirror_f.__name__),
            )
            mocks[
                f"on_{remove_suffix(topic).replace('.', '_').replace('-', '_')}"
            ] = getattr(self.mocks, mirror_f.__name__)
            awaited_mocks[
                f"on_{remove_suffix(topic).replace('.', '_').replace('-', '_')}"
            ] = getattr(self.awaited_mocks, mirror_f.__name__)

    AppMocks = collections.namedtuple(  # type: ignore
        f"{self.__class__.__name__}Mocks", [f_name for f_name in mocks]
    )
    setattr(self, "mocks", AppMocks(**mocks))
    setattr(self, "awaited_mocks", AppMocks(**awaited_mocks))
