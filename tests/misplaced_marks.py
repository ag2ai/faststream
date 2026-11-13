import ast
from functools import cache
from pathlib import Path

import pytest

# Run as `pytest --collect-only -q -m "" -p tests.misplaced_marks`: marks decide which
# CI job runs a test, so a wrong one drops it from a job without failing anything.
TESTS_ROOT = Path(__file__).parent

BROKERS = ("kafka", "confluent", "rabbit", "nats", "redis", "mqtt")


@pytest.hookimpl(trylast=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    problems = sorted({
        problem
        for item in session.items
        if isinstance(item, pytest.Function)
        for problem in (
            _connected_in_memory(item),
            _missing_broker_mark(item),
            _unmarked_broker_import(item),
        )
        if problem
    })

    if problems:
        pytest.exit("Misplaced marks:\n" + "\n".join(problems), returncode=1)


def _connected_in_memory(item: pytest.Function) -> str | None:
    if item.cls is None or not any(
        base.__name__.endswith("MemoryTestcaseConfig") for base in item.cls.__mro__
    ):
        return None

    # a single test of an in-memory class may still open a real connection
    # (`with_real=True`), so only a mark covering the whole class is wrong
    for node, _ in item.iter_markers_with_node("connected"):
        if node is not item:
            return (
                f"{_location(item)}::{item.cls.__name__} -- in-memory class under "
                "`connected`, mark the tests that reach a broker instead"
            )

    return None


def _missing_broker_mark(item: pytest.Function) -> str | None:
    directories = item.path.relative_to(TESTS_ROOT).parts[:-1]

    for broker in BROKERS:
        if broker not in directories:
            continue

        position = directories.index(broker)
        in_cluster = directories[position : position + 2] == ("redis", "cluster")
        mark = "redis_cluster" if in_cluster else broker

        if item.get_closest_marker(mark) is None:
            return f"{_location(item)} -- under `{broker}/`, not marked `{mark}`"

    return None


def _unmarked_broker_import(item: pytest.Function) -> str | None:
    # an unmarked test still runs in `test-basic`; a `connected` one only runs
    # in its broker's job, so a missing mark there means it never runs at all
    if item.get_closest_marker("connected") is None:
        return None

    # a module that imports several brokers uses one of them as an example;
    # only a module built on a single broker names the job that must run it
    brokers = _imported_brokers(item.path)
    if len(brokers) != 1:
        return None

    for broker in brokers:
        if broker == "redis" and item.get_closest_marker("redis_cluster") is not None:
            continue

        if item.get_closest_marker(broker) is None:
            return (
                f"{_location(item)} -- imports `faststream.{broker}`, "
                f"not marked `{broker}`"
            )

    return None


@cache
def _imported_brokers(path: Path) -> frozenset[str]:
    modules: list[str] = []
    for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
        if isinstance(node, ast.Import):
            modules.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            modules.append(node.module)

    return frozenset(
        parts[1]
        for module in modules
        if (parts := module.split("."))[0] == "faststream"
        and len(parts) > 1
        and parts[1] in BROKERS
    )


def _location(item: pytest.Function) -> str:
    return str(item.path.relative_to(TESTS_ROOT.parent))
