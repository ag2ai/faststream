from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING

import watchfiles

from faststream._internal.cli.supervisors.basereload import BaseReload
from faststream._internal.logger import logger

if TYPE_CHECKING:
    from faststream._internal.cli.dto import RunArgs, RunFunction


class ExtendedFilter(watchfiles.PythonFilter):
    """A class that extends the `watchfiles.PythonFilter` class."""

    def __init__(
        self,
        *,
        ignore_paths: Sequence[str | Path] | None = None,
        extra_extensions: Sequence[str] = (),
    ) -> None:
        super().__init__(ignore_paths=ignore_paths, extra_extensions=extra_extensions)
        self.ignore_dirs: Sequence[str] = (
            *self.ignore_dirs,
            "venv",
            "env",
            ".github",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
            "__pycache__",
        )


class WatchReloader(BaseReload):
    """A class to reload a target function when files in specified directories change."""

    def __init__(
        self,
        target: "RunFunction",
        args: "RunArgs",
        reload_dirs: Sequence[Path | str],
        reload_delay: float = 0.3,
        extra_extensions: Sequence[str] = (),
    ) -> None:
        super().__init__(target, args, reload_delay)
        self.reloader_name = "WatchFiles"
        self.reload_dirs = reload_dirs
        self.watcher = watchfiles.watch(
            *reload_dirs,
            step=int(reload_delay * 1000),
            watch_filter=ExtendedFilter(extra_extensions=extra_extensions),
            stop_event=self.should_exit,
            yield_on_timeout=True,
        )

    def startup(self) -> None:
        logger.info(
            "Will watch for changes in these directories: %s",
            [str(i) for i in self.reload_dirs],
        )
        super().startup()

    def should_restart(self) -> bool:
        for changes in self.watcher:  # pragma: no branch
            if changes:  # pragma: no branch
                unique_paths = {str(Path(c[1]).name) for c in changes}
                logger.info(
                    "WatchReloader detected file change in '%s'. Reloading...",
                    ", ".join(unique_paths),
                )
                return True
        return False  # pragma: no cover
