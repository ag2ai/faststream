from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated, Any, NewType
from unittest.mock import MagicMock

import pytest
from fast_depends import Depends, Provider

from faststream import FastStream
from faststream.nats import NatsBroker

HOOKS = ("on_startup", "after_startup", "on_shutdown", "after_shutdown")


@pytest.mark.asyncio()
@pytest.mark.parametrize("hook_name", HOOKS)
async def test_lifecycle_hook_uses_app_dependency_provider(
    hook_name: str, mock: MagicMock
) -> None:
    provider = Provider()
    app = FastStream(NatsBroker(), provider=provider)

    Dep = NewType("Dep", Any)
    received: list[Any] = []

    async def hook(dep: Annotated[Dep, Depends(Dep)]) -> None:
        received.append(dep)

    getattr(app, hook_name)(hook)

    with provider.scope(Dep, lambda: mock):
        for func in getattr(app, f"_{hook_name}_calling"):
            await func()

    assert received == [mock]


@pytest.mark.asyncio()
async def test_lifespan_uses_app_dependency_provider(mock: MagicMock) -> None:
    provider = Provider()
    Dep = NewType("Dep", Any)
    received: list[Any] = []

    @asynccontextmanager
    async def lifespan(dep: Annotated[Dep, Depends(Dep)]) -> AsyncGenerator[None, None]:
        received.append(dep)
        yield

    app = FastStream(NatsBroker(), provider=provider, lifespan=lifespan)

    with provider.scope(Dep, lambda: mock):
        async with app.lifespan_context():
            pass

    assert received == [mock]
