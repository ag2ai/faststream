import random

import httpx
import psutil
import pytest

from tests.marks import skip_windows

from .conftest import FastStreamCLIFactory, GenerateTemplateFactory


@pytest.mark.slow()
@skip_windows
def test_run(
    generate_template: GenerateTemplateFactory,
    faststream_cli: FastStreamCLIFactory,
) -> None:
    app_code = """
    import json

    from faststream import FastStream, specification
    from faststream.rabbit import RabbitBroker
    from faststream.asgi import AsgiResponse, get, make_asyncapi_asgi

    CONTEXT = {}

    @get
    async def context(scope):
        return AsgiResponse(json.dumps(CONTEXT).encode(), status_code=200)

    broker = RabbitBroker()
    app = FastStream(broker).as_asgi(
        asgi_routes=[
            ("/context", context),
            ("/liveness", AsgiResponse(b"hello world", status_code=200)),
            ("/docs", make_asyncapi_asgi(specification.AsyncAPI(broker))),
        ],
    )

    @app.on_startup
    async def start(test: int, port: int) -> None:
        CONTEXT["test"] = test
        CONTEXT["port"] = port
    """
    port = random.randrange(40000, 65535)
    extra_param = random.randrange(1, 100)

    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--port",
            f"{port}",
            "--test",
            f"{extra_param}",
        ),
    ):
        # Test liveness
        response = httpx.get(f"http://127.0.0.1:{port}/liveness")
        assert response.text == "hello world"
        assert response.status_code == 200

        # Test documentation
        response = httpx.get(f"http://127.0.0.1:{port}/docs")
        assert response.text.strip().startswith("<!DOCTYPE html>")
        assert len(response.text) > 1200

        # Test extra context
        response = httpx.get(f"http://127.0.0.1:{port}/context")
        assert response.json() == {"test": extra_param, "port": port}
        assert response.status_code == 200


@pytest.mark.slow()
@skip_windows
def test_single_worker(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from faststream.asgi import AsgiFastStream, AsgiResponse
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    app = AsgiFastStream(broker, asgi_routes=[
        ("/liveness", AsgiResponse(b"hello world", status_code=200)),
    ])
    """
    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            "1",
        ),
    ):
        response = httpx.get("http://127.0.0.1:8000/liveness")
        assert response.text == "hello world"
        assert response.status_code == 200


@pytest.mark.slow()
@skip_windows
def test_many_workers(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from faststream.asgi import AsgiFastStream
    from faststream.nats import NatsBroker

    app = AsgiFastStream(NatsBroker())
    """

    workers = 2

    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app",
            "--workers",
            str(workers)
        ) as cli_thread,
    ):
        assert cli_thread.process

        process = psutil.Process(pid=cli_thread.process.pid)
        assert len(process.children()) == workers + 1  # 1 for the main process


@pytest.mark.slow()
@skip_windows
def test_factory(
    generate_template: GenerateTemplateFactory, faststream_cli: FastStreamCLIFactory
) -> None:
    app_code = """
    from faststream.asgi import AsgiFastStream, AsgiResponse, get
    from faststream.nats import NatsBroker

    broker = NatsBroker()

    def app_factory():
        return AsgiFastStream(broker, asgi_routes=[
            ("/liveness", AsgiResponse(b"hello world", status_code=200)),
        ])
    """

    with (
        generate_template(app_code) as app_path,
        faststream_cli(
            "faststream",
            "run",
            f"{app_path.stem}:app_factory",
            "--factory",
        ),
    ):
        response = httpx.get("http://127.0.0.1:8000/liveness")
        assert response.text == "hello world"
        assert response.status_code == 200
