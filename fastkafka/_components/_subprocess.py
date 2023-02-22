# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/006_Subprocess.ipynb.

# %% auto 0
__all__ = ['logger', 'terminate_asyncio_process', 'run_async_subprocesses']

# %% ../../nbs/006_Subprocess.ipynb 1
# import importlib
# import sys
import asyncio
from typing import *
from contextlib import contextmanager
from pathlib import Path
import threading
import signal
from contextlib import ExitStack, contextmanager
from tempfile import TemporaryDirectory


import multiprocessing

# from fastcore.meta import delegates
from fastcore.basics import patch
import typer
import asyncer

# from fastkafka.application import FastKafka
# from fastkafka.testing import change_dir
# from fastkafka._components.helpers import _import_from_string, generate_app_src
from .logger import get_logger

# %% ../../nbs/006_Subprocess.ipynb 5
logger = get_logger(__name__)

# %% ../../nbs/006_Subprocess.ipynb 7
async def terminate_asyncio_process(p: asyncio.subprocess.Process) -> None:
    """Terminate an asyncio process.

    Args:
        p: The process to terminate

    Returns:
        None

    Raises:
        ValueError: If s1 or s2 is None

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
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

# %% ../../nbs/006_Subprocess.ipynb 9
async def run_async_subprocesses(
    commands: List[str], commands_args: List[List[Any]], *, sleep_between: int = 0
) -> None:
    """Run a list of subprocesses in parallel.

    Args:
        commands: A list of commands to run.
        commands_args: A list of lists of arguments for each command.
        sleep_between: The number of seconds to sleep between each command.

    Returns:
        None

    Raises:
        ValueError: If commands and commands_args are not of the same length.
        RuntimeError: If stdout is not piped.
        typer.Exit: If the return codes are not all zero.

    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    loop = asyncio.get_event_loop()

    HANDLED_SIGNALS = (
        signal.SIGINT,  # Unix signal 2. Sent by Ctrl+C.
        signal.SIGTERM,  # Unix signal 15. Sent by `kill <pid>`.
    )

    d = {"should_exit": False}

    def handle_exit(sig: int, d: Dict[str, bool] = d) -> None:
        """Handle exit signal

        Args:
            sig: The signal number
            d: A dictionary with a key "should_exit"

        Returns:
            None

        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
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
        """Log the output of a process.

        Args:
            output: The output of a process
            pid: The process id
            d: A dictionary

        Returns:
            None

        Raises:
            RuntimeError: If output is None

        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
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
