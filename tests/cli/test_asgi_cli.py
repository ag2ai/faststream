import json
import random
import urllib.request

import pytest

from faststream._compat import IS_WINDOWS


@pytest.mark.slow
@pytest.mark.skipif(IS_WINDOWS, reason="does not run on windows")
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
            with urllib.request.urlopen(  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                f"http://127.0.0.1:{port}/liveness"
            ) as response:
                assert response.read().decode() == "hello world"
                assert response.getcode() == 200

            with urllib.request.urlopen(  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                f"http://127.0.0.1:{port}/docs"
            ) as response:
                content = response.read().decode()
                assert content.strip().startswith("<!DOCTYPE html>")
                assert len(content) > 1200

            with urllib.request.urlopen(  # nosemgrep: python.lang.security.audit.dynamic-urllib-use-detected.dynamic-urllib-use-detected
                f"http://127.0.0.1:{port}/context"
            ) as response:
                data = json.loads(response.read().decode())
                assert data == {"test": extra_param, "port": port}
                assert response.getcode() == 200
