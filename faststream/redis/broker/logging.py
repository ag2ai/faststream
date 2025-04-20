import logging
from functools import partial
from typing import TYPE_CHECKING, Optional

from faststream._internal.log.logging import get_broker_logger
from faststream._internal.state.logger import (
    DefaultLoggerStorage,
    make_logger_state,
)

if TYPE_CHECKING:
    from faststream._internal.basic_types import AnyDict, LoggerProto
    from faststream._internal.context import ContextRepo


class RedisParamsStorage(DefaultLoggerStorage):
    def __init__(
        self,
        log_fmt: Optional[str],
    ) -> None:
        super().__init__(log_fmt)

        self._max_channel_name = 4

        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        self.logger_log_level = level

    def setup_log_contest(self, params: "AnyDict") -> None:
        self._max_channel_name = max(
            (
                self._max_channel_name,
                len(params.get("channel", "")),
            ),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        message_id_ln = 10

        # TODO: generate unique logger names to not share between brokers
        if not (lg := self._get_logger_ref()):
            lg = get_broker_logger(
                name="redis",
                default_context={
                    "channel": "",
                },
                message_id_ln=message_id_ln,
                fmt=self._log_fmt
                or (
                    "%(asctime)s %(levelname)-8s - "
                    f"%(channel)-{self._max_channel_name}s | "
                    f"%(message_id)-{message_id_ln}s "
                    "- %(message)s"
                ),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_redis_logger_state = partial(
    make_logger_state,
    default_storage_cls=RedisParamsStorage,
)
