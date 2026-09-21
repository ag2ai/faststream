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
        for problem in (_connected_in_memory(item), _missing_broker_mark(item))
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


def _location(item: pytest.Function) -> str:
    return str(item.path.relative_to(TESTS_ROOT.parent))
