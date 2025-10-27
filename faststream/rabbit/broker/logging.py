from functools import partial
from typing import TYPE_CHECKING, Any

from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


class RabbitParamsStorage(DefaultLoggerStorage):
    def __init__(self) -> None:
        super().__init__()

        self._max_exchange_len = 4
        self._max_queue_len = 4

    def register_subscriber(self, params: dict[str, Any]) -> None:
        self._max_exchange_len = max(
            self._max_exchange_len,
            len(params.get("exchange", "")),
        )
        self._max_queue_len = max(
            self._max_queue_len,
            len(params.get("queue", "")),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        # TODO: generate unique logger names to not share between brokers
        if not (lg := self._get_logger_ref()):
            message_id_ln = 10

            lg = get_broker_logger(
                name="rabbit",
                default_context={
                    "queue": "",
                    "exchange": "",
                },
                message_id_ln=message_id_ln,
                fmt=(
                    "%(asctime)s %(levelname)-8s - "
                    f"%(exchange)-{self._max_exchange_len}s | "
                    f"%(queue)-{self._max_queue_len}s | "
                    f"%(message_id)-{message_id_ln}s "
                    "- %(message)s"
                ),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_rabbit_logger_state = partial(
    make_logger_state,
    default_storage_cls=RabbitParamsStorage,
)
