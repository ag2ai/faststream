import asyncio
from typing import Any, Generator, List
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from faststream.__about__ import __version__
from faststream.annotations import ContextRepo
from faststream.utils import context as global_context


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(
    excinfo: pytest.ExceptionInfo[KeyboardInterrupt],
) -> None:  # pragma: no cover
    pytest.mark.skip("Interrupted Test Session")


def pytest_collection_modifyitems(items: List[pytest.Item]) -> None:
    for item in items:
        item.add_marker("all")


@pytest.fixture
def queue() -> str:
    return str(uuid4())


@pytest.fixture
def event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture(scope="session")
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture
def mock() -> Generator[MagicMock, Any, None]:
    m = MagicMock()
    yield m
    m.reset_mock()


@pytest.fixture
def async_mock() -> Generator[AsyncMock, Any, None]:
    m = AsyncMock()
    yield m
    m.reset_mock()


@pytest.fixture(scope="session")
def version() -> str:
    return __version__


@pytest.fixture
def context() -> Generator[ContextRepo, Any, None]:
    yield global_context
    global_context.clear()


@pytest.fixture
def kafka_basic_project() -> str:
    return "docs.docs_src.kafka.basic.basic:app"
