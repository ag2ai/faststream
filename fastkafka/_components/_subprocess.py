# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/022_Subprocess.ipynb.

# %% auto 0
__all__ = ['logger', 'terminate_asyncio_process', 'run_async_subprocesses']

# %% ../../nbs/022_Subprocess.ipynb 1
# import importlib
# import sys
import asyncio
import multiprocessing
import signal
import threading
from contextlib import ExitStack, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import *

import asyncer
import typer
from fastcore.basics import patch

from .logger import get_logger

# %% ../../nbs/022_Subprocess.ipynb 5
logger = get_logger(__name__)

# %% ../../nbs/022_Subprocess.ipynb 7
async def terminate_asyncio_process(p: asyncio.subprocess.Process) -> None:
    logger.info(f"terminate_asyncio_process(): Terminating the process {p.pid}...")
    # Check if SIGINT already propagated to process
    try:
        await asyncio.wait_for(p.wait(), 1)
        logger.info(
            f"terminate_asyncio_process(): Process {p.pid} was already terminated."
        )
        return
    except asyncio.TimeoutError:
        pass

    for i in range(3):
        p.terminate()
        try:
            await asyncio.wait_for(p.wait(), 10)
            logger.info(f"terminate_asyncio_process(): Process {p.pid} terminated.")
            return
        except asyncio.TimeoutError:
            logger.warning(
                f"terminate_asyncio_process(): Process {p.pid} not terminated, retrying..."
            )

    logger.warning(f"Killing the process {p.pid}...")
    p.kill()
    await p.wait()
    logger.warning(f"terminate_asyncio_process(): Process {p.pid} killed!")

# %% ../../nbs/022_Subprocess.ipynb 9
async def run_async_subprocesses(
    commands: List[str], commands_args: List[List[Any]], *, sleep_between: int = 0
) -> None:
    loop = asyncio.get_event_loop()

    HANDLED_SIGNALS = (
        signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
        signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
    )

    d = {"should_exit": False}

    def handle_exit(sig: int, d: Dict[str, bool] = d) -> None:
        d["should_exit"] = True

    for sig in HANDLED_SIGNALS:
        loop.add_signal_handler(sig, handle_exit, sig)

    async with asyncer.create_task_group() as tg:
        tasks = []
        for cmd, args in zip(commands, commands_args):
            tasks.append(
                tg.soonify(asyncio.create_subprocess_exec)(
                    cmd,
                    *args,
                    stdout=asyncio.subprocess.PIPE,
                    stdin=asyncio.subprocess.PIPE,
                )
            )
            await asyncio.sleep(sleep_between)

    procs = [task.value for task in tasks]

    async def log_output(
        output: Optional[asyncio.StreamReader], pid: int, d: Dict[str, bool] = d
    ) -> None:
        if output is None:
            raise RuntimeError("Expected StreamReader, got None. Is stdout piped?")
        while not output.at_eof():
            outs = await output.readline()
            if outs != b"":
                typer.echo(f"[{pid:03d}]: " + outs.decode("utf-8"), nl=False)

    async with asyncer.create_task_group() as tg:
        for proc in procs:
            tg.soonify(log_output)(proc.stdout, proc.pid)

        while not d["should_exit"]:
            await asyncio.sleep(0.2)

        typer.echo("Starting process cleanup, this may take a few seconds...")
        for proc in procs:
            tg.soonify(terminate_asyncio_process)(proc)

    for proc in procs:
        output, _ = await proc.communicate()
        if output:
            typer.echo(f"[{proc.pid:03d}]: " + output.decode("utf-8"), nl=False)

    returncodes = [proc.returncode for proc in procs]
    if not returncodes == [0] * len(procs):
        typer.secho(
            f"Return codes are not all zero: {returncodes}",
            err=True,
            fg=typer.colors.RED,
        )
        raise typer.Exit(1)
