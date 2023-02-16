# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/9991_Test_Client.ipynb.

# %% auto 0
__all__ = ['Tester', 'mirror_producer', 'mirror_consumer']

# %% ../../nbs/9991_Test_Client.ipynb 1
from collections import namedtuple
from unittest.mock import AsyncMock, MagicMock
from typing import *
import inspect
import functools

from fastcore.foundation import patch

from ..application import FastKafka

# %% ../../nbs/9991_Test_Client.ipynb 3
@patch
def create_mocks(self: FastKafka) -> None:
    app_methods = [f for f, _ in self._consumers_store.values()] + [
        f for f, _, _ in self._producers_store.values()
    ]
    self.AppMocks = namedtuple(
        f"{self.__class__.__name__}Mocks", [f.__name__ for f in app_methods]
    )
    # todo: create Magicmock if needed
    self.mocks = self.AppMocks(**{f.__name__: AsyncMock() for f in app_methods})

    def add_mock(f: Callable[[...], Any], mock: AsyncMock) -> Callable[[...], Any]:
        @functools.wraps(f)
        async def async_inner(
            *args, f: Callable[[...], Any] = f, mock: AsyncMock = mock, **kwargs
        ) -> Any:
            await mock(*args, **kwargs)
            return await f(*args, **kwargs)

        return async_inner

    self._consumers_store = {
        name: (
            add_mock(f, getattr(self.mocks, f.__name__)),
            kwargs,
        )
        for name, (f, kwargs) in self._consumers_store.items()
    }
    self._producers_store = {
        name: (
            add_mock(f, getattr(self.mocks, f.__name__)),
            producer,
            kwargs,
        )
        for name, (f, producer, kwargs) in self._producers_store.items()
    }

# %% ../../nbs/9991_Test_Client.ipynb 6
class Tester(FastKafka):
    def __init__(self, apps: List[FastKafka]):
        self.apps = apps
        super().__init__(
            bootstrap_servers=self.apps[0]._kafka_config["bootstrap_servers"]
        )
        self.create_mirror()

    async def startup(self):
        for app in self.apps:
            app.create_mocks()
            await app.startup()

        self.create_mocks()
        await super().startup()
        await asyncio.sleep(3)

    async def shutdown(self):
        await super().shutdown()
        for app in self.apps[::-1]:
            await app.shutdown()

    def create_mirror(self):
        pass

# %% ../../nbs/9991_Test_Client.ipynb 9
def mirror_producer(
    topic: str, producer_f: Callable[[...], Any]
) -> Callable[[...], Any]:
    msg_type = inspect.signature(producer_f).return_annotation
    print(msg_type)

    def skeleton_func(msg):
        pass

    mirror_func = skeleton_func
    sig = inspect.signature(skeleton_func)

    # adjust name
    mirror_func.__name__ = "on_" + topic

    # adjust arg and return val
    sig = sig.replace(
        parameters=[
            inspect.Parameter(
                name="msg",
                annotation=msg_type,
                kind=inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]
    )

    mirror_func.__signature__ = sig
    return mirror_func

# %% ../../nbs/9991_Test_Client.ipynb 11
def mirror_consumer(
    topic: str, consumer_f: Callable[[...], Any]
) -> Callable[[...], Any]:
    msg_type = inspect.signature(consumer_f).parameters["msg"]

    def skeleton_func(msg):
        return msg

    mirror_func = skeleton_func
    sig = inspect.signature(skeleton_func)

    # adjust name
    mirror_func.__name__ = "to_" + topic

    # adjust arg and return val
    sig = sig.replace(parameters=[msg_type], return_annotation=msg_type.annotation)

    mirror_func.__signature__ = sig
    return mirror_func

# %% ../../nbs/9991_Test_Client.ipynb 15
@patch
def create_mirror(self: Tester):
    pass
