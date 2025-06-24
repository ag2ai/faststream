import pytest

from .conftest import FastStreamCLIFactory, GenerateTemplateFactory


@pytest.mark.slow()
def test_run(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
) -> None:
    app_code = """
    from faststream import FastStream
    from faststream.nats import NatsBroker

    app = FastStream(NatsBroker())
    """
    with (
        generate_template(app_code) as app_path,
        faststream_cli("faststream", "run", f"{app_path.stem}:app") as cli_thread,
    ):
        pass

    assert cli_thread.process.returncode == 0
