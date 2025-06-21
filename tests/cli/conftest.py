import os
import subprocess
import threading
import time
from contextlib import contextmanager
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    ContextManager,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
)

import pytest

from faststream import FastStream

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def broker():
    # separate import from e2e tests
    from faststream.rabbit import RabbitBroker

    return RabbitBroker()


@pytest.fixture
def app_without_logger(broker):
    return FastStream(broker, None)


@pytest.fixture
def app_without_broker():
    return FastStream()


@pytest.fixture
def app(broker):
    return FastStream(broker)


class GenerateTemplateFactory(Protocol):
    def __call__(
        self, code: str, filename: str = "temp_app.py"
    ) -> ContextManager["Path"]: ...


@pytest.fixture
def generate_template(
    tmp_path: "Path",
) -> GenerateTemplateFactory:
    @contextmanager
    def factory(
        code: str, filename: str = "temp_app.py"
    ) -> Generator["Path", None, None]:
        temp_dir = tmp_path / "faststream_templates"
        temp_dir.mkdir(exist_ok=True)

        file_path: Path = temp_dir / filename
        cleaned_code = dedent(code).strip()

        file_path.write_text(cleaned_code)

        try:
            yield file_path
        finally:
            file_path.unlink(missing_ok=True)

    return factory


class CliThread(Protocol):
    process: Optional[subprocess.Popen]

    def stop(self) -> None: ...


class FastStreamCLIFactory(Protocol):
    def __call__(
        self,
        cmd: List[str],
        wait_time: float = 1.5,
        extra_env: Optional[Dict[str, str]] = None,
    ) -> ContextManager[CliThread]: ...


@pytest.fixture
def faststream_cli(tmp_path: "Path") -> FastStreamCLIFactory:
    @contextmanager
    def factory(
        cmd: List[str],
        wait_time: float = 1.5,
        extra_env: Optional[Dict[str, str]] = None,
    ) -> Generator[CliThread, None, None]:
        class RealCLIThread(threading.Thread):
            def __init__(self, command: List[str], env: Dict[str, str]):
                super().__init__()
                self.command = command
                self.process: Optional[subprocess.Popen] = None
                self.env = env

            def run(self) -> None:
                self.process = subprocess.Popen(
                    self.command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=False,
                    env=self.env,
                )
                self.process.wait()

            def stop(self) -> None:
                if self.process:
                    self.process.terminate()
                    try:
                        self.process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        self.process.kill()

        extra_env = extra_env or {}
        env = os.environ.copy()
        env.update(**extra_env)
        cli = RealCLIThread(cmd, extra_env)
        cli.start()
        time.sleep(wait_time)

        try:
            yield cli
        finally:
            cli.stop()
            cli.join()

    return factory
