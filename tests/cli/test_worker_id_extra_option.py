import time
from pathlib import Path

import pytest

from tests.cli import interfaces
from tests.marks import skip_windows


@skip_windows
@pytest.mark.slow()
@pytest.mark.parametrize(
    ("app_import"),
    (
        pytest.param(
            "from faststream import FastStream",
            id="default_app",
        ),
        pytest.param(
            "from faststream.asgi import AsgiFastStream",
            id="asgi_app",
        ),
    ),
)
@pytest.mark.parametrize(
    ("expected_worker_ids", "cli_options"),
    (
        pytest.param(
            ["None"],
            [],
            id="single_worker",
        ),
        pytest.param(
            ["0", "1"],
            ["--workers", "2"],
            id="many_workers",
            marks=[
                pytest.mark.flaky(reruns=3, reruns_delay=1),
                pytest.mark.slow(),
            ],
        ),
    ),
)
def test_worker_id_parameter_exists(
    generate_template: interfaces.GenerateTemplateFactory,
    faststream_cli: interfaces.FastStreamCLIFactory,
    tmp_path: Path,
    app_import: str,
    expected_worker_ids: list[str],
    cli_options: list[str],
) -> None:
    marker_dir = tmp_path / "worker_markers"
    marker_dir.mkdir()

    app_code = f"""
    import os
    from pathlib import Path

    {app_import} as FastStreamApp
    from faststream.nats import NatsBroker

    broker = NatsBroker()
    app = FastStreamApp(broker)

    @app.on_startup
    def write_worker_marker(worker_id):
        Path(os.environ["WORKER_MARKER_DIR"], f"worker_{{worker_id}}").touch()
    """

    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            *cli_options,
            extra_env={"WORKER_MARKER_DIR": str(marker_dir)},
        ),
    ):
        expected_markers = {f"worker_{wid}" for wid in expected_worker_ids}
        deadline = time.time() + 15
        seen: set[str] = set()

        while time.time() < deadline:
            seen = {p.name for p in marker_dir.iterdir()}
            if expected_markers.issubset(seen):
                return
            time.sleep(0.1)

        msg = f"Expected markers {expected_markers}, got {seen}"
        raise AssertionError(msg)
