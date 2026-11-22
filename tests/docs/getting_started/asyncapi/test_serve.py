import json
import shlex
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yaml
from typer.testing import CliRunner

from faststream._internal.cli.main import cli
from tests.marks import require_aiokafka

# the page saves the application as `basic.py` and runs the commands beside it
APP = "docs.docs_src.kafka.basic.basic:app"


def run_command(runner: CliRunner, command: str) -> None:
    # `faststream docs gen basic:app` -> ["docs", "gen", "<the snippet>"]
    args = [arg.replace("basic:app", APP) for arg in shlex.split(command)[1:]]

    result = runner.invoke(cli, args)
    assert result.exit_code == 0, result.output


def expected_schema() -> Any:
    from docs.docs_src.kafka.basic.basic import app

    return app.schema.to_specification().to_jsonable()


@pytest.mark.kafka()
@require_aiokafka
def test_gen(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from docs.docs_src.getting_started.asyncapi.serve import (
        gen_asyncapi_json_cmd,
        gen_asyncapi_yaml_cmd,
    )

    monkeypatch.chdir(tmp_path)
    run_command(runner, gen_asyncapi_json_cmd)
    run_command(runner, gen_asyncapi_yaml_cmd)

    assert json.loads((tmp_path / "asyncapi.json").read_text()) == expected_schema()
    assert yaml.safe_load((tmp_path / "asyncapi.yaml").read_text()) == expected_schema()


@pytest.mark.kafka()
@require_aiokafka
def test_serve(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from docs.docs_src.getting_started.asyncapi.serve import (
        asyncapi_serve_cmd,
        asyncapi_serve_json_cmd,
        asyncapi_serve_yaml_cmd,
        gen_asyncapi_json_cmd,
        gen_asyncapi_yaml_cmd,
    )

    monkeypatch.chdir(tmp_path)
    run_command(runner, gen_asyncapi_json_cmd)
    run_command(runner, gen_asyncapi_yaml_cmd)

    served = MagicMock()
    with patch("faststream._internal.cli.docs.serve_app", served):
        run_command(runner, asyncapi_serve_cmd)
        run_command(runner, asyncapi_serve_json_cmd)
        run_command(runner, asyncapi_serve_yaml_cmd)

    # the application and both exported files are served as the same document
    assert [call.args[0].to_jsonable() for call in served.call_args_list] == [
        expected_schema(),
        expected_schema(),
        expected_schema(),
    ]
