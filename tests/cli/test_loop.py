import pytest

from tests.cli import interfaces
from tests.marks import skip_windows


@pytest.fixture()
def app_code() -> str:
    return """
    import asyncio
    import sys

    from faststream._internal.application import Application


    class MockApplication(Application):
        async def run(self, *args, **kwargs):
            loop = asyncio.get_event_loop()
            print(f"{loop.__module__}:{loop.__class__.__name__}".encode(), file=sys.stderr)


    app = MockApplication()
    """


@pytest.mark.slow()
@skip_windows
@pytest.mark.parametrize(
    ("loop_param", "expected_loop"),
    (
        pytest.param(
            "asyncio:new_event_loop",
            "asyncio.unix_events:_UnixSelectorEventLoop",
            id="asyncio",
        ),
        pytest.param(
            "uvloop:new_event_loop",
            "uvloop:Loop",
            id="uvloop",
        ),
        pytest.param(
            "auto",
            "uvloop:Loop",
            id="auto uvloop",
        ),
    ),
)
def test_loop(
    loop_param: str,
    expected_loop: str,
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
    app_code: str,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            "--loop",
            loop_param,
            f"{app_path.stem}:app",
        ) as cli,
    ):
        assert cli.wait_for_stderr(expected_loop)


@pytest.mark.slow()
@skip_windows
def test_loop_bad_format(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
    app_code: str,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            "--loop",
            "not_found",
            f"{app_path.stem}:app",
        ) as cli,
    ):
        cli.wait_for_stderr(
            'Invalid value for \'--loop\': Import string "not_found" must be in format "<module>:<attribute>"'
        )


@pytest.mark.slow()
@skip_windows
def test_loop_not_found(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
    app_code: str,
) -> None:
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            "--loop",
            "loop:not_found",
            f"{app_path.stem}:app",
        ) as cli,
    ):
        cli.wait_for_stderr(
            "Invalid value for '--loop': Please, input module like [python_file:docs_object] or [module:attribute]"
        )
