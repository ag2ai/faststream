from unittest.mock import Mock

import pytest
from typer.testing import CliRunner

from faststream._internal.cli.main import cli
from faststream.app import FastStream


@pytest.fixture()
def confluent_basic_project() -> str:
    return "docs.docs_src.confluent.basic.basic:app"


@pytest.mark.confluent()
def test_run_cmd(
    runner: CliRunner,
    mock: Mock,
    monkeypatch: pytest.MonkeyPatch,
    confluent_basic_project,
) -> None:
    async def patched_run(self: FastStream, *args, **kwargs) -> None:
        await self.start()
        await self.stop()
        mock()

    with monkeypatch.context() as m:
        m.setattr(FastStream, "run", patched_run)
        r = runner.invoke(
            cli,
            [
                "run",
                confluent_basic_project,
            ],
        )

    assert r.exit_code == 0
    mock.assert_called_once()
