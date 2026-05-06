import json
from collections.abc import Callable
from pathlib import Path
from typing import Any, TextIO

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from faststream._internal.cli.main import cli
from tests.cli import interfaces
from tests.marks import require_aiokafka, skip_macos, skip_windows

json_asyncapi_doc = """
{
  "asyncapi": "2.6.0",
  "defaultContentType": "application/json",
  "info": {
    "title": "FastStream",
    "version": "0.1.0"
  },
  "servers": {
    "development": {
      "url": "localhost:9092",
      "protocol": "kafka",
      "protocolVersion": "auto"
    }
  },
  "channels": {
    "input_data:OnInputData": {
      "servers": [
        "development"
      ],
      "bindings": {
        "kafka": {
          "topic": "input_data",
          "bindingVersion": "0.4.0"
        }
      },
      "subscribe": {
        "message": {
          "$ref": "#/components/messages/input_data:OnInputData:Message"
        }
      }
    }
  },
  "components": {
    "messages": {
      "input_data:OnInputData:Message": {
        "title": "input_data:OnInputData:Message",
        "correlationId": {
          "location": "$message.header#/correlation_id"
        },
        "payload": {
          "$ref": "#/components/schemas/DataBasic"
        }
      }
    },
    "schemas": {
      "DataBasic": {
        "properties": {
          "data": {
            "type": "number"
          }
        },
        "required": [
          "data"
        ],
        "title": "DataBasic",
        "type": "object"
      }
    }
  }
}
"""

yaml_asyncapi_doc = """
asyncapi: 2.6.0
defaultContentType: application/json
info:
  title: FastStream
  version: 0.1.0
  description: ''
servers:
  development:
    url: 'localhost:9092'
    protocol: kafka
    protocolVersion: auto
channels:
  'input_data:OnInputData':
    servers:
      - development
    bindings: null
    kafka:
      topic: input_data
      bindingVersion: 0.4.0
    subscribe: null
    message:
      $ref: '#/components/messages/input_data:OnInputData:Message'
components:
  messages:
    'input_data:OnInputData:Message':
      title: 'input_data:OnInputData:Message'
      correlationId:
        location: '$message.header#/correlation_id'
      payload:
        $ref: '#/components/schemas/DataBasic'
  schemas:
    DataBasic:
      properties:
        data: null
        title: Data
        type: number
      required:
        - data
      title: DataBasic
      type: object
"""


app_code = """
from pydantic import BaseModel, Field, NonNegativeFloat

from faststream import FastStream, Logger
from faststream.specification import AsyncAPI
from faststream.kafka import KafkaBroker


class DataBasic(BaseModel):
    data: NonNegativeFloat = Field(
        ..., examples=[0.5], description="Float data example"
    )


broker = KafkaBroker("localhost:9092")
app = FastStream(broker, specification=AsyncAPI())


@broker.publisher("output_data")
@broker.subscriber("input_data")
async def on_input_data(msg: DataBasic, logger: Logger) -> DataBasic:
    logger.info(msg)
    return DataBasic(data=msg.data + 1.0)
"""


@pytest.mark.slow()
@require_aiokafka
@skip_windows
@pytest.mark.parametrize(
    ("commands", "load_schema"),
    (
        pytest.param(
            [],
            json.load,
            id="json",
        ),
        pytest.param(
            ["--yaml"],
            lambda f: yaml.load(f, Loader=yaml.BaseLoader),
            id="yaml",
        ),
    ),
)
def test_gen_asyncapi_for_kafka_app(
    commands: list[str],
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
    load_schema: Callable[[TextIO], Any],
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "docs",
            "gen",
            f"{app_path.stem}:app",
            "--out",
            str(app_path.parent / "schema.json"),
            *commands,
        ) as cli_thread,
    ):
        cli_thread.wait_for_stderr("Your project AsyncAPI scheme")

        assert cli_thread.process

        schema_path = app_path.parent / "schema.json"
        assert schema_path.exists()

        with schema_path.open() as f:
            schema = load_schema(f)

        assert schema
        schema_path.unlink()


@pytest.mark.slow()
@skip_windows
def test_gen_wrong_path(faststream_cli) -> None:
    with faststream_cli("faststream", "docs", "gen", "non_existent:app") as cli:
        assert cli.wait_for_stderr("No such file or directory")


@skip_windows
@skip_macos  # MacOS GHArunner doesn't allow to run 0.0.0.0 process
@require_aiokafka
@pytest.mark.slow()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
def test_serve_asyncapi_docs_from_app(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream", "docs", "serve", "--host", "0.0.0.0", f"{app_path.stem}:app"
        ) as cli,
    ):
        cli.wait_for_stderr("Please, do not use it in production.")

        try:
            response = httpx.get("http://0.0.0.0:8000")
        except Exception as e:
            raise RuntimeError(cli.stderr) from e

        assert "<title>FastStream AsyncAPI</title>" in response.text
        assert response.status_code == 200


@skip_windows
@require_aiokafka
def test_serve_asyncapi_docs_from_app_with_reload(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "docs",
            "serve",
            f"{app_path.stem}:app",
            "--reload",
        ) as cli,
    ):
        assert cli.wait_for_stderr("HTTPServer running on http://localhost:8000"), (
            cli.stderr
        )


@skip_windows
@skip_macos  # MacOS GHA runner doesn't allow to run 0.0.0.0 process
@require_aiokafka
@pytest.mark.slow()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
@pytest.mark.parametrize(
    ("doc_filename", "doc"),
    (
        pytest.param("asyncapi.json", json_asyncapi_doc, id="json_schema"),
        pytest.param("asyncapi.yaml", yaml_asyncapi_doc, id="yaml_schema"),
    ),
)
def test_serve_asyncapi_docs_from_file(
    doc_filename: str,
    doc: str,
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
) -> None:
    with (
        generate_template(doc, filename=doc_filename) as doc_path,
        faststream_cli(
            "faststream", "docs", "serve", "--host", "0.0.0.0", str(doc_path)
        ) as cli,
    ):
        cli.wait_for_stderr("Please, do not use it in production.")

        try:
            response = httpx.get("http://0.0.0.0:8000")
        except Exception as e:
            raise RuntimeError(cli.stderr) from e

        assert "<title>FastStream AsyncAPI</title>" in response.text
        assert response.status_code == 200


@skip_windows
@skip_macos  # MacOS GHA runner doesn't allow to run 0.0.0.0 process
@require_aiokafka
@pytest.mark.slow()
@pytest.mark.flaky(reruns=3, reruns_delay=1)
def test_serve_asyncapi_docs_with_explicit_yaml_parser(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
) -> None:
    """End-to-end: `--yaml-parser pyyaml` actually serves the document.

    The base ``test_serve_asyncapi_docs_from_file`` covers the default
    (``auto``) parser path. This one exercises the explicit flag so a
    regression in flag plumbing — flag parsed but ignored, or wired to the
    wrong loader — gets caught against a real server.
    """
    with (
        generate_template(yaml_asyncapi_doc, filename="asyncapi.yaml") as doc_path,
        faststream_cli(
            "faststream",
            "docs",
            "serve",
            "--host",
            "0.0.0.0",
            "--yaml-parser",
            "pyyaml",
            str(doc_path),
        ) as cli,
    ):
        cli.wait_for_stderr("Please, do not use it in production.")

        try:
            response = httpx.get("http://0.0.0.0:8000")
        except Exception as e:
            raise RuntimeError(cli.stderr) from e

        assert response.status_code == 200
        assert "<title>FastStream AsyncAPI</title>" in response.text


# Tests for #2709: improved error message + pluggable parser


@pytest.fixture()
def stderr_runner() -> CliRunner:
    """A CliRunner that captures stderr separately so tests can assert on it."""
    return CliRunner(mix_stderr=False)


def test_invalid_yaml_schema_error_includes_pydantic_detail(
    tmp_path: Path,
    stderr_runner: CliRunner,
) -> None:
    """A schema that loads as YAML but fails Pydantic validation.

    The CLI should surface the validation error to stderr, not the bare
    ``not supported`` message.
    """
    bad_doc = """
asyncapi: 2.6.0
info:
  title: BadDoc
"""
    p = tmp_path / "bad.yaml"
    p.write_text(bad_doc)

    result = stderr_runner.invoke(
        cli,
        ["docs", "serve", str(p)],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    # Old behavior: only "not supported" appears.
    # New behavior: also surface the per-version validation error.
    assert "not supported" in result.stderr
    assert "AsyncAPI 3.0.0 validation errors" in result.stderr
    assert "AsyncAPI 2.6.0 validation errors" in result.stderr


def test_yaml_parser_option_unknown_name_reports_clearly(
    tmp_path: Path,
    stderr_runner: CliRunner,
) -> None:
    """Passing `--yaml-parser <bogus>` should fail with a clear message."""
    p = tmp_path / "doc.yaml"
    p.write_text(yaml_asyncapi_doc)

    result = stderr_runner.invoke(
        cli,
        ["docs", "serve", str(p), "--yaml-parser", "totally-fake"],
        catch_exceptions=False,
    )
    assert result.exit_code == 1
    assert "Unknown YAML parser" in result.stderr
    assert "totally-fake" in result.stderr


def test_unquoted_scalar_hint_is_emitted(
    tmp_path: Path,
    stderr_runner: CliRunner,
) -> None:
    """Surface a hint when YAML resolves a bare scalar to a native type.

    ``protocolVersion: 3.2`` (no quotes) becomes ``float`` under every
    standard YAML parser, but AsyncAPI requires a string. The CLI should
    point at the quoting issue rather than letting the user guess.
    """
    bad_doc = """
asyncapi: 3.0.0
info:
  title: BadDoc
  version: 0.1.0
servers:
  development:
    host: 'localhost:9092'
    pathname: /
    protocol: kafka
    protocolVersion: 3.2
"""
    p = tmp_path / "bad.yaml"
    p.write_text(bad_doc)

    result = stderr_runner.invoke(cli, ["docs", "serve", str(p)], catch_exceptions=False)
    assert result.exit_code == 1
    assert "Hint" in result.stderr
    assert "quote the value" in result.stderr


def test_yaml_parser_pyyaml_loads_valid_schema(
    tmp_path: Path,
    stderr_runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`--yaml-parser pyyaml` actually loads a well-formed file.

    Negative tests cover the failure paths; this is the positive path so a
    regression that breaks ``get_yaml_parser`` resolution gets caught.
    ``serve_app`` is patched out because it blocks on a real socket bind.
    """
    p = tmp_path / "doc.yaml"
    p.write_text(yaml_asyncapi_doc)

    served: list[object] = []
    monkeypatch.setattr(
        "faststream._internal.cli.docs.serve_app",
        lambda raw, host, port: served.append((raw, host, port)),
    )

    result = stderr_runner.invoke(
        cli,
        ["docs", "serve", str(p), "--yaml-parser", "pyyaml"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.stderr
    assert len(served) == 1
    assert "Failed to parse" not in result.stderr
    assert "not supported" not in result.stderr


def test_malformed_yaml_emits_parse_error(
    tmp_path: Path,
    stderr_runner: CliRunner,
) -> None:
    """Malformed YAML produces a parse-time error.

    The error path is distinct from the validation-error path so users
    can tell the difference between "your YAML is broken" and "your
    AsyncAPI is broken".
    """
    p = tmp_path / "broken.yaml"
    p.write_text("this is: : invalid: yaml:\n  ::\n")

    result = stderr_runner.invoke(cli, ["docs", "serve", str(p)], catch_exceptions=False)
    assert result.exit_code == 1
    assert "Failed to parse" in result.stderr
