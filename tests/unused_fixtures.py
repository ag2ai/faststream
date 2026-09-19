import ast
import inspect
import textwrap
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

# Run as `pytest --collect-only -q -m "" -p tests.unused_fixtures`: collection sees
# connected tests without a broker, so nothing here depends on what a CI job ran.
TESTS_ROOT = Path(__file__).parent


@pytest.hookimpl(trylast=True)
def pytest_collection_finish(session: pytest.Session) -> None:
    problems = [
        *(f"{name} -- never requested" for name in _never_requested(session)),
        *(f"{name} -- requested, never read" for name in _requested_not_read(session)),
    ]

    if problems:
        pytest.exit("Unused fixtures:\n" + "\n".join(problems), returncode=1)


def _never_requested(session: pytest.Session) -> list[str]:
    used = {
        fixturedef
        for item in session.items
        for fixturedefs in item._fixtureinfo.name2fixturedefs.values()
        for fixturedef in fixturedefs
    }

    return sorted(
        f"{fixturedef.argname} @ {_location(fixturedef.func)}"
        for fixturedefs in session._fixturemanager._arg2fixturedefs.values()
        for fixturedef in fixturedefs
        if fixturedef not in used
        and TESTS_ROOT in Path(inspect.getfile(fixturedef.func)).parents
    )


def _requested_not_read(session: pytest.Session) -> list[str]:
    # a fixture wanted for its side effect belongs in `@pytest.mark.usefixtures`
    unread: dict[str, list[str]] = {}

    for item in session.items:
        func = getattr(item, "function", None)
        # decorators such as `freeze_time` wrap the test in their own module
        func = func and inspect.unwrap(func)
        if func is None or TESTS_ROOT not in Path(inspect.getfile(func)).parents:
            continue

        location = _location(func)
        if location in unread:
            continue

        fixtures = set(item._fixtureinfo.argnames) - {"request"}
        if callspec := getattr(item, "callspec", None):
            fixtures -= set(callspec.params)

        node = ast.parse(textwrap.dedent(inspect.getsource(func))).body[0]
        read = {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}
        unread[location] = sorted(fixtures - read)

    return sorted(
        f"{', '.join(names)} @ {location}" for location, names in unread.items() if names
    )


def _location(func: Callable[..., Any] | None) -> str:
    if func is None:
        return ""
    path = Path(inspect.getfile(func)).relative_to(TESTS_ROOT.parent)
    return f"{path}:{inspect.getsourcelines(func)[1]}"
