import asyncio
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from typer.testing import CliRunner

from faststream.__about__ import __version__
from faststream._internal.context import ContextRepo


@pytest.hookimpl(tryfirst=True)
def pytest_keyboard_interrupt(excinfo) -> None:  # pragma: no cover
    pytest.mark.skip("Interrupted Test Session")


def pytest_collection_modifyitems(items) -> None:
    for item in items:
        item.add_marker("all")


@pytest.fixture()
def queue() -> str:
    return str(uuid4())


@pytest.fixture()
def event() -> asyncio.Event:
    return asyncio.Event()


@pytest.fixture(scope="session")
def runner() -> CliRunner:
    return CliRunner()


@pytest.fixture()
def mock() -> MagicMock:
    m = MagicMock()
    yield m
    m.reset_mock()


@pytest.fixture()
def async_mock() -> AsyncMock:
    m = AsyncMock()
    yield m
    m.reset_mock()


@pytest.fixture(scope="session")
def version() -> str:
    return __version__


@pytest.fixture()
def context() -> ContextRepo:
    return ContextRepo()


@pytest.fixture()
def kafka_basic_project() -> str:
    return "docs.docs_src.kafka.basic.basic:app"


@pytest.fixture()
def kafka_ascynapi_project() -> str:
    return "docs.docs_src.kafka.basic.basic:asyncapi"
