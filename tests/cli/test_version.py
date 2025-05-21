import platform

from faststream._internal.cli.main import cli


def test_version(runner, version) -> None:
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert version in result.stdout
    assert platform.python_implementation() in result.stdout
    assert platform.python_version() in result.stdout
    assert platform.system() in result.stdout
