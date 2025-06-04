import traceback
from typing import Any
from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from faststream._internal.cli.main import cli
from faststream.app import FastStream


@pytest.mark.kafka()
def test_run_cmd(
    runner: CliRunner,
    mock: Mock,
    monkeypatch: pytest.MonkeyPatch,
    kafka_basic_project: str,
) -> None:
    async def patched_run(self: FastStream, *args: Any, **kwargs: Any) -> None:
        await self.start()
        await self.stop()
        mock()

    with monkeypatch.context() as m:
        m.setattr(FastStream, "run", patched_run)
        r = runner.invoke(
            cli,
            [
                "run",
                kafka_basic_project,
            ],
        )

    assert r.exit_code == 0, (r.output, traceback.format_exception(r.exception))
    mock.assert_called_once()
