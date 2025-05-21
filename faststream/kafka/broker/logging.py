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


class KafkaParamsStorage(DefaultLoggerStorage):
    def __init__(
        self,
        log_fmt: Optional[str],
    ) -> None:
        super().__init__(log_fmt)

        self._max_topic_len = 4
        self._max_group_len = 0

        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        self.logger_log_level = level

    def setup_log_contest(self, params: "AnyDict") -> None:
        self._max_topic_len = max(
            (
                self._max_topic_len,
                len(params.get("topic", "")),
            ),
        )
        self._max_group_len = max(
            (
                self._max_group_len,
                len(params.get("group_id", "")),
            ),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        message_id_ln = 10

        # TODO: generate unique logger names to not share between brokers
        if not (lg := self._get_logger_ref()):
            lg = get_broker_logger(
                name="kafka",
                default_context={
                    "topic": "",
                    "group_id": "",
                },
                message_id_ln=message_id_ln,
                fmt=self._log_fmt
                or "".join((
                    "%(asctime)s %(levelname)-8s - ",
                    f"%(topic)-{self._max_topic_len}s | ",
                    (
                        f"%(group_id)-{self._max_group_len}s | "
                        if self._max_group_len
                        else ""
                    ),
                    f"%(message_id)-{message_id_ln}s ",
                    "- %(message)s",
                )),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_kafka_logger_state = partial(
    make_logger_state,
    default_storage_cls=KafkaParamsStorage,
)
