import pytest

from faststream._internal._compat import IS_WINDOWS
from tests.cli import interfaces


@pytest.mark.slow()
def test_run(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
) -> None:
    app_code = """
    from faststream import FastStream
    from faststream.nats import NatsBroker

    app = FastStream(NatsBroker())
    """
    with (
        generate_template(app_code) as app_path,
        faststream_cli("faststream", "run", f"{app_path.stem}:app") as cli,
    ):
        cli.signint()
        cli.wait(3.0)

    if IS_WINDOWS:
        assert cli.process.returncode == 1
    else:
        assert cli.process.returncode == 0
