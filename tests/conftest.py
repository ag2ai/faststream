import asyncio
from typing import Any, Generator, List, Mapping
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


@pytest.fixture
def config() -> Mapping[str, Any]:
    return {
        "compression.type": "gzip",
        "leader.replication.throttled.replicas": "0:1",
        "min.insync.replicas": 2,
        "message.downconversion.enable": False,
        "segment.jitter.ms": 1000,
        "cleanup.policy": "compact",
        "flush.ms": 60000,
        "follower.replication.throttled.replicas": "0:1",
        "segment.bytes": 134217728,
        "retention.ms": 259200000,
        "flush.messages": 100000,
        "message.format.version": "2.7-IV1",
        "max.compaction.lag.ms": 86400000,
        "file.delete.delay.ms": 120000,
        "max.message.bytes": 2000000,
        "min.compaction.lag.ms": 1000,
        "message.timestamp.type": "LogAppendTime",
        "preallocate": True,
        "index.interval.bytes": 2048,
        "min.cleanable.dirty.ratio": 0.4,
        "unclean.leader.election.enable": True,
        "retention.bytes": 10737418240,
        "delete.retention.ms": 43200000,
        "segment.ms": 259200000,
        "message.timestamp.difference.max.ms": 86400000,
        "segment.index.bytes": 5242880,
    }
