import re
from pathlib import Path

import pytest


def _read_pyproject() -> str:
    for parent in Path(__file__).resolve().parents:
        pyproject = parent / "pyproject.toml"
        if pyproject.exists():
            return pyproject.read_text(encoding="utf-8")
    msg = "pyproject.toml not found"
    raise FileNotFoundError(msg)


@pytest.mark.nats()
def test_nats_extra_requires_nkeys() -> None:
    """The ``nats`` extra must install ``nats-py`` with its ``nkeys`` extra.

    Without it, NKEY authentication (e.g. the ``nkeys_seed`` argument) fails at
    runtime with ``ModuleNotFoundError: No module named 'nkeys'`` (issue #2673).
    """
    content = _read_pyproject()

    match = re.search(
        r"^\s*nats\s*=\s*(?P<deps>\[.*\])\s*$",
        content,
        re.MULTILINE,
    )
    assert match, "no 'nats' optional-dependency declaration found in pyproject.toml"

    assert re.search(r"nats-py\s*\[[^\]]*\bnkeys\b[^\]]*\]", match.group("deps")), (
        "the 'nats' extra must require 'nats-py[nkeys]' so NKEY auth works; "
        f"got: {match.group('deps').strip()}"
    )
