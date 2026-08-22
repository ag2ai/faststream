import logging
from typing import Any, Optional

from faststream._compat import override
from faststream.broker.core.mixins import LoggingMixin
from faststream.broker.message import StreamMessage
from faststream.log import access_logger
from faststream.types import AnyDict


class SQSLoggingMixin(LoggingMixin):
    _max_queue_len: int

    def __init__(
        self,
        *args: Any,
        logger: Optional[logging.Logger] = access_logger,
        log_level: int = logging.INFO,
        log_fmt: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *args,
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            **kwargs,
        )
        self._max_queue_len = 4

    @override
    def _get_log_context(  # type: ignore[override]
        self,
        message: Optional[StreamMessage[Any]],
        queue: str = "",
    ) -> AnyDict:
        return {
            "queue": queue,
            **super()._get_log_context(message),
        }

    @property
    def fmt(self) -> str:
        return self._fmt or (
            "%(asctime)s %(levelname)s - "
            f"%(queue)-{self._max_queue_len}s | "
            "%(message_id)-10s "
            "- %(message)s"
        )

    def _setup_log_context(
        self,
        queue: Optional[str] = None,
    ) -> None:
        if queue is not None:
            self._max_queue_len = max((self._max_queue_len, len(queue)))
