import os
import select
import signal
import subprocess
import threading
import time
from collections.abc import Generator
from contextlib import contextmanager, suppress
from pathlib import Path
from textwrap import dedent

import pytest

from faststream import FastStream
from faststream._internal._compat import IS_WINDOWS
from tests.cli import interfaces


@pytest.fixture()
def broker():
    # separate import from e2e tests
    from faststream.rabbit import RabbitBroker

    return RabbitBroker()


@pytest.fixture()
def app_without_logger(broker) -> FastStream:
    return FastStream(broker, logger=None)


@pytest.fixture()
def app_without_broker() -> FastStream:
    return FastStream()


@pytest.fixture()
def app(broker) -> FastStream:
    return FastStream(broker)


@pytest.fixture()
def faststream_tmp_path(tmp_path: "Path"):
    faststream_tmp = tmp_path / "faststream_templates"
    faststream_tmp.mkdir(exist_ok=True)
    return faststream_tmp


@pytest.fixture()
def generate_template(
    faststream_tmp_path: "Path",
) -> interfaces.GenerateTemplateFactory:
    @contextmanager
    def factory(
        code: str,
        filename: str = "temp_app.py",
    ) -> Generator["Path", None, None]:
        file_path: Path = faststream_tmp_path / filename
        cleaned_code = dedent(code).strip()

        file_path.write_text(cleaned_code, encoding="utf-8")

        try:
            yield file_path
        finally:
            file_path.unlink(missing_ok=True)

    return factory


class CLIThread:
    def __init__(
        self,
        command: tuple[str, ...],
        env: dict[str, str],
    ) -> None:
        self.process = subprocess.Popen(
            command,
            stderr=subprocess.PIPE,
            text=True,
            shell=False,
            env=env,
        )
        self.running = True
        self.started = False

        self.stderr = ""

        self.__std_poll_thread = threading.Thread(target=self._poll_std)
        self.__std_poll_thread.start()

    def _poll_std(self) -> None:
        assert self.process.stderr

        if IS_WINDOWS:
            return

        while self.running:
            rlist, _, _ = select.select([self.process.stderr], [], [], 0.1)
            if rlist:
                self.started = True

                if line := self.process.stderr.readline():
                    self.stderr += line.strip()

                else:
                    break

            elif self.process.poll() is not None:
                break

    def wait_for_stderr(self, message: str, timeout: float = 2.0) -> bool:
        assert self.process.stderr

        if message in self.stderr:
            return True

        expiration_time = time.time() + timeout

        while time.time() < expiration_time:
            time.sleep(0.1)
            if message in self.stderr:
                return True

        if self.process.returncode is not None:
            return message in self.process.stderr.read()

        return False

    def wait(self, timeout: float) -> None:
        self.process.wait(timeout)

    def signint(self) -> None:
        if IS_WINDOWS:
            self.process.terminate()
        else:
            self.process.send_signal(signal.SIGINT)

    def stop(self) -> None:
        self.process.terminate()

        self.running = False
        with suppress(Exception):
            self.__std_poll_thread.join()

        try:
            self.wait(5)

        except subprocess.TimeoutExpired:
            self.process.kill()


@pytest.fixture()
def faststream_cli(
    faststream_tmp_path: Path,
) -> interfaces.FastStreamCLIFactory:
    @contextmanager
    def cli_factory(
        *cmd: str,
        wait_time: float = 2.0,
        extra_env: dict[str, str] | None = None,
    ) -> Generator[CLIThread, None, None]:
        env = (
            os.environ.copy()
            | {
                "PATH": f"{faststream_tmp_path}:{os.environ['PATH']}",
                "PYTHONPATH": str(faststream_tmp_path),
            }
            | (extra_env or {})
        )

        cli = CLIThread(cmd, env)

        wait_for_startup(cli, wait_time)

        try:
            yield cli
        finally:
            cli.stop()

    return cli_factory


def wait_for_startup(
    cli: CLIThread,
    timeout: float = 10,
    check_interval: float = 0.1,
) -> None:
    start_time = time.time()

    while time.time() - start_time < timeout:
        if cli.started:
            return

        if cli.process.poll() is not None:
            return

        time.sleep(check_interval)

    return
