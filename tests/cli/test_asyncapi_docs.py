import json
import sys
from http.server import HTTPServer
from pathlib import Path
from unittest.mock import Mock

import pytest
import yaml
from typer.testing import CliRunner

from docs.docs_src.getting_started.asyncapi.serve import (
    gen_json_cmd,
    gen_yaml_cmd,
    serve_cmd,
)
from faststream.cli.main import cli
from tests.marks import require_aiokafka

GEN_JSON_CMD = gen_json_cmd.split(" ")[1:-1]
GEN_YAML_CMD = gen_yaml_cmd.split(" ")[1:-1]
SERVE_CMD = serve_cmd.split(" ")[1:-1]


@require_aiokafka
def test_gen_asyncapi_json_for_kafka_app(runner: CliRunner, kafka_basic_project: Path):
    r = runner.invoke(
        cli, [*GEN_JSON_CMD, "--out", "schema.json", str(kafka_basic_project)]
    )
    assert r.exit_code == 0

    schema_path = Path.cwd() / "schema.json"
    assert schema_path.exists()

    with schema_path.open("r") as f:
        schema = json.load(f)

    assert schema
    schema_path.unlink()


@require_aiokafka
def test_gen_asyncapi_yaml_for_kafka_app(runner: CliRunner, kafka_basic_project: Path):
    r = runner.invoke(cli, GEN_YAML_CMD + [str(kafka_basic_project)])  # noqa: RUF005
    assert r.exit_code == 0

    schema_path = Path.cwd() / "asyncapi.yaml"
    assert schema_path.exists()

    with schema_path.open("r") as f:
        schema = yaml.load(f, Loader=yaml.BaseLoader)

    assert schema
    schema_path.unlink()


def test_gen_wrong_path(runner: CliRunner):
    r = runner.invoke(cli, GEN_JSON_CMD + ["basic:app1"])  # noqa: RUF005
    assert r.exit_code == 2

    if r.stdout:  # click <= 8.2.0
        assert "No such file or directory" in r.stdout

    else:
        assert "No such file or directory" in r.stderr


@require_aiokafka
def test_serve_asyncapi_docs(
    runner: CliRunner,
    kafka_basic_project: Path,
    monkeypatch,
    mock: Mock,
):
    with monkeypatch.context() as m:
        m.setattr(HTTPServer, "serve_forever", mock)
        r = runner.invoke(cli, SERVE_CMD + [str(kafka_basic_project)])  # noqa: RUF005

    assert r.exit_code == 0
    mock.assert_called_once()


@require_aiokafka
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_serve_asyncapi_json_schema(
    runner: CliRunner,
    kafka_basic_project: Path,
    monkeypatch,
    mock: Mock,
):
    r = runner.invoke(cli, GEN_JSON_CMD + [str(kafka_basic_project)])  # noqa: RUF005
    schema_path = Path.cwd() / "asyncapi.json"

    with monkeypatch.context() as m:
        m.setattr(HTTPServer, "serve_forever", mock)
        r = runner.invoke(cli, SERVE_CMD + [str(schema_path)])  # noqa: RUF005

    assert r.exit_code == 0
    mock.assert_called_once()

    schema_path.unlink()


@require_aiokafka
@pytest.mark.skipif(sys.platform == "win32", reason="does not run on windows")
def test_serve_asyncapi_yaml_schema(
    runner: CliRunner,
    kafka_basic_project: Path,
    monkeypatch,
    mock: Mock,
):
    r = runner.invoke(cli, GEN_YAML_CMD + [str(kafka_basic_project)])  # noqa: RUF005
    schema_path = Path.cwd() / "asyncapi.yaml"

    with monkeypatch.context() as m:
        m.setattr(HTTPServer, "serve_forever", mock)
        r = runner.invoke(cli, SERVE_CMD + [str(schema_path)])  # noqa: RUF005

    assert r.exit_code == 0
    mock.assert_called_once()

    schema_path.unlink()
