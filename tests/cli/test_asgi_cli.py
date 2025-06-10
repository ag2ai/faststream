import random

import pytest
import requests


@pytest.mark.slow
def test_run_asgi(generate_template, faststream_cli) -> None:
    app_code = """
    import json

    from faststream import FastStream
    from faststream.nats import NatsBroker
    from faststream.asgi import AsgiResponse, get

    broker = NatsBroker()

    @get
    async def liveness_ping(scope):
        return AsgiResponse(b"hello world", status_code=200)


    CONTEXT = {}

    @get
    async def context(scope):
        return AsgiResponse(json.dumps(CONTEXT).encode(), status_code=200)


    app = FastStream(broker).as_asgi(
        asgi_routes=[
            ("/liveness", liveness_ping),
            ("/context", context)
        ],
        asyncapi_path="/docs",
    )

    @app.on_startup
    async def start(test: int, port: int):
        CONTEXT["test"] = test
        CONTEXT["port"] = port

    """
    with generate_template(app_code) as app_path:
        module_name = str(app_path).replace(".py", "")
        port = random.randrange(40000, 65535)
        extra_param = random.randrange(1, 100)

        with faststream_cli(
            [
                "faststream",
                "run",
                f"{module_name}:app",
                "--port",
                f"{port}",
                "--test",
                f"{extra_param}",
            ]
        ):
            r = requests.get(f"http://127.0.0.1:{port}/liveness")
            assert r.text == "hello world"
            assert r.status_code == 200

            r = requests.get(f"http://127.0.0.1:{port}/docs")
            assert r.text.strip().startswith("<!DOCTYPE html>")
            assert len(r.text) > 1200

            r = requests.get(f"http://127.0.0.1:{port}/context")
            assert r.json() == {"test": extra_param, "port": port}
            assert r.status_code == 200
