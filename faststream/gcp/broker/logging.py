"""GCP Pub/Sub logging utilities."""

import logging
from functools import partial
from typing import TYPE_CHECKING, Any

from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger

if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo


class GCPParamsStorage(DefaultLoggerStorage):
    def __init__(self) -> None:
        super().__init__()

        self._max_topic_name = 4
        self._max_subscription_name = 4

        self.logger_log_level = logging.INFO

    def set_level(self, level: int) -> None:
        self.logger_log_level = level

    def register_subscriber(self, params: dict[str, Any]) -> None:
        self._max_topic_name = max(
            (
                self._max_topic_name,
                len(params.get("topic", "")),
            ),
        )
        self._max_subscription_name = max(
            (
                self._max_subscription_name,
                len(params.get("subscription", "")),
            ),
        )

    def get_logger(self, *, context: "ContextRepo") -> "LoggerProto":
        message_id_ln = 10

        if not (lg := self._get_logger_ref()):
            lg = get_broker_logger(
                name="gcp",
                default_context={
                    "topic": "",
                    "subscription": "",
                },
                message_id_ln=message_id_ln,
                fmt=(
                    "%(asctime)s %(levelname)-8s - "
                    f"%(topic)-{self._max_topic_name}s | "
                    f"%(subscription)-{self._max_subscription_name}s | "
                    f"%(message_id)-{message_id_ln}s "
                    "- %(message)s"
                ),
                context=context,
                log_level=self.logger_log_level,
            )
            self._logger_ref.add(lg)

        return lg


make_gcp_logger_state = partial(
    make_logger_state,
    default_storage_cls=GCPParamsStorage,
)
