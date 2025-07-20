from pathlib import Path

import pytest
from typer import BadParameter

from faststream._internal.cli.utils.imports import (
    _get_obj_path,
    _import_object,
    import_from_string,
)
from faststream.app import FastStream
from tests.marks import require_aiokafka, require_aiopika, require_nats


def test_import_wrong() -> None:
    dir, app = _get_obj_path("tests:test_object")
    with pytest.raises(FileNotFoundError):
        _import_object(dir, app)


@pytest.mark.parametrize(
    ("test_input", "exp_module", "exp_app"),
    (
        pytest.param(
            "module:app",
            "module",
            "app",
            id="simple",
        ),
        pytest.param(
            "module.module.module:app",
            "module/module/module",
            "app",
            id="nested init",
        ),
    ),
)
def test_get_app_path(test_input: str, exp_module: str, exp_app: str) -> None:
    dir, app = _get_obj_path(test_input)
    assert app == exp_app
    assert dir == Path.cwd() / exp_module


def test_get_app_path_wrong() -> None:
    with pytest.raises(ValueError, match=r"`module.app` is not a path to object"):
        _get_obj_path("module.app")


def test_import_from_string_import_wrong() -> None:
    with pytest.raises(BadParameter):
        import_from_string("tests:test_object")


@pytest.mark.parametrize(
    ("test_input", "exp_module"),
    (
        pytest.param("examples.kafka.testing:app", "examples/kafka/testing.py"),
        pytest.param("examples.nats.e01_basic:app", "examples/nats/e01_basic.py"),
        pytest.param("examples.rabbit.topic:app", "examples/rabbit/topic.py"),
    ),
)
@require_nats
@require_aiopika
@require_aiokafka
def test_import_from_string(test_input: str, exp_module: str) -> None:
    module, app = import_from_string(test_input)
    assert isinstance(app, FastStream)
    assert module == (Path.cwd() / exp_module).parent


@pytest.mark.parametrize(
    ("test_input", "exp_module"),
    (
        pytest.param(
            "examples.kafka:app",
            "examples/kafka/__init__.py",
            id="kafka init",
        ),
        pytest.param(
            "examples.nats:app",
            "examples/nats/__init__.py",
            id="nats init",
        ),
        pytest.param(
            "examples.rabbit:app",
            "examples/rabbit/__init__.py",
            id="rabbit init",
        ),
    ),
)
@require_nats
@require_aiopika
@require_aiokafka
def test_import_module(test_input: str, exp_module: str) -> None:
    module, app = import_from_string(test_input)
    assert isinstance(app, FastStream)
    assert module == (Path.cwd() / exp_module).parent


def test_import_from_string_wrong() -> None:
    with pytest.raises(BadParameter):
        import_from_string("module.app")
