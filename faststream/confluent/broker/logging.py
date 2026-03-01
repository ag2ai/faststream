import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


class KafkaParamsStorage(DefaultLoggerStorage):
    def __init__(self) -> None:
        super().__init__()

        self._max_topic_len = 4
        self._max_group_len = 0

        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        self.logger_log_level = level

    def register_subscriber(self, params: dict[str, Any]) -> None:
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
                name="confluent",
                default_context={
                    "topic": "",
                    "group_id": "",
                },
                message_id_ln=message_id_ln,
                fmt="".join((
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
