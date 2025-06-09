import logging
import sys
from typing import Literal

COLORED_LEVELS = {
    logging.DEBUG: "\033[36mDEBUG\033[0m",
    logging.INFO: "\033[32mINFO\033[0m",
    logging.WARNING: "\033[33mWARNING\033[0m",
    logging.ERROR: "\033[31mERROR\033[0m",
    logging.CRITICAL: "\033[91mCRITICAL\033[0m",
}


class ColourizedFormatter(logging.Formatter):
    """A class to format log messages with colorized level names.

    Methods:
        __init__ : Initialize the formatter with specified format strings.
        formatMessage : Format the log record message with colorized level name.
    """

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        use_colors: bool | None = None,
    ) -> None:
        """Initialize the formatter with specified format strings.

        Initialize the formatter either with the specified format string, or a
        default as described above. Allow for specialized date formatting with
        the optional datefmt argument. If datefmt is omitted, you get an
        ISO8601-like (or RFC 3339-like) format.

        Use a style parameter of '%', '{' or '$' to specify that you want to
        use one of %-formatting, :meth:`str.format` (``{}``) formatting or
        :class:`string.Template` formatting in your format string.
        """
        if use_colors in {True, False}:
            self.use_colors = use_colors
        else:
            self.use_colors = sys.stdout.isatty()
        super().__init__(fmt=fmt, datefmt=datefmt, style=style)

    def formatMessage(self, record: logging.LogRecord) -> str:  # noqa: N802
        """Formats the log message.

        Args:
            record (logging.LogRecord): The log record to format.

        Returns:
            str: The formatted log message.
        """
        if self.use_colors:
            record.levelname = expand_log_field(
                field=COLORED_LEVELS.get(record.levelno, record.levelname),
                symbols=17,
            )

        return super().formatMessage(record)


def expand_log_field(field: str, symbols: int) -> str:
    """Expands a log field by adding spaces.

    Args:
        field: The log field to expand.
        symbols: The desired length of the expanded field.

    Returns:
        The expanded log field.
    """
    return field + (" " * (symbols - len(field)))
