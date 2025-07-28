import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


class NatsParamsStorage(DefaultLoggerStorage):
    def __init__(self) -> None:
        super().__init__()

        self._max_queue_len = 0
        self._max_stream_len = 0
        self._max_subject_len = 4

        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        self.logger_log_level = level

    def register_subscriber(self, params: dict[str, Any]) -> None:
        self._max_subject_len = max(
            (
                self._max_subject_len,
                len(params.get("subject", "")),
            ),
        )
        self._max_queue_len = max(
            (
                self._max_queue_len,
                len(params.get("queue", "")),
            ),
        )
        self._max_stream_len = max(
            (
                self._max_stream_len,
                len(params.get("stream", "")),
            ),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        # TODO: generate unique logger names to not share between brokers
        if not (lg := self._get_logger_ref()):
            message_id_ln = 10

            lg = get_broker_logger(
                name="nats",
                default_context={
                    "subject": "",
                    "stream": "",
                    "queue": "",
                },
                message_id_ln=message_id_ln,
                fmt="".join((
                    "%(asctime)s %(levelname)-8s - ",
                    (
                        f"%(stream)-{self._max_stream_len}s | "
                        if self._max_stream_len
                        else ""
                    ),
                    (
                        f"%(queue)-{self._max_queue_len}s | "
                        if self._max_queue_len
                        else ""
                    ),
                    f"%(subject)-{self._max_subject_len}s | ",
                    f"%(message_id)-{message_id_ln}s - ",
                    "%(message)s",
                )),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_nats_logger_state = partial(
    make_logger_state,
    default_storage_cls=NatsParamsStorage,
)
