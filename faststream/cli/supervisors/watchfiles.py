from pathlib import Path
from typing import Any, Optional, Sequence, Tuple, Union

import watchfiles

from faststream.cli.supervisors.basereload import BaseReload
from faststream.log import logger
from faststream.types import DecoratedCallable


class ExtendedFilter(watchfiles.PythonFilter):
    """A class that extends the `watchfiles.PythonFilter` class.

    Attributes:
        ignore_dirs : Tuple of directories to ignore

    Methods:
        __init__ : Initializes the `ExtendedFilter` object
            Args:
                ignore_paths : Optional sequence of paths to ignore
                extra_extensions : Sequence of extra extensions to include

            Returns:
                None
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    ignore_dirs: Tuple[str, ...]

    def __init__(
        self,
        *,
        ignore_paths: Optional[Sequence[Union[str, Path]]] = None,
        extra_extensions: Sequence[str] = (),
    ) -> None:
        """Initialize the class.

        Args:
            ignore_paths: Optional sequence of paths to ignore.
            extra_extensions: Sequence of extra extensions.

        Returns:
            None
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        super().__init__(ignore_paths=ignore_paths, extra_extensions=extra_extensions)
        self.extensions = self.extensions + (".env", ".yaml")
        self.ignore_dirs = self.ignore_dirs + (
            "venv",
            "env",
            ".github",
            ".mypy_cache",
            ".pytest_cache",
            ".ruff_cache",
        )


class WatchReloader(BaseReload):
    """A class to reload a target function when files in specified directories change.

    Attributes:
        target : the function to be reloaded
        args : arguments to be passed to the target function
        reload_dirs : directories to watch for file changes
        reload_delay : delay in seconds between each check for file changes

    Methods:
        should_restart() -> bool: Checks if any files in the watched directories have changed and returns True if a change is detected, False otherwise.
    !!! note

        The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
    """
    def __init__(
        self,
        target: DecoratedCallable,
        args: Tuple[Any, ...],
        reload_dirs: Sequence[Union[Path, str]],
        reload_delay: float = 0.3,
    ) -> None:
        """Initialize a WatchFilesReloader object.

        Args:
            target: The target callable to be executed.
            args: The arguments to be passed to the target callable.
            reload_dirs: A sequence of directories to watch for changes.
            reload_delay: The delay in seconds between checking for changes. Default is 0.3.

        Returns:
            None.
        !!! note

            The above docstring is autogenerated by docstring-gen library (https://docstring-gen.airt.ai)
        """
        super().__init__(target, args, reload_delay)
        self.reloader_name = "WatchFiles"
        self.watcher = watchfiles.watch(
            *reload_dirs,
            step=int(reload_delay * 1000),
            watch_filter=ExtendedFilter(),
            stop_event=self.should_exit,
            yield_on_timeout=True,
        )

    def should_restart(self) -> bool:
        for changes in self.watcher:  # pragma: no branch
            if changes:  # pragma: no branch
                unique_paths = {Path(c[1]).name for c in changes}
                message = "WatchReloader detected file change in '%s'. Reloading..."
                logger.info(message % tuple(unique_paths))
                return True
        return False  # pragma: no cover
