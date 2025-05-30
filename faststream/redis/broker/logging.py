import logging
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from typing_extensions import Annotated, deprecated

from faststream.broker.core.usecase import BrokerUsecase
from faststream.log.logging import get_broker_logger
from faststream.redis.message import UnifyRedisDict
from faststream.types import EMPTY

if TYPE_CHECKING:
    from redis.asyncio.client import Redis  # noqa: F401

    from faststream.types import LoggerProto


class RedisLoggingBroker(BrokerUsecase[UnifyRedisDict, "Redis[bytes]"]):
    """A class that extends the LoggingMixin class and adds additional functionality for logging Redis related information."""

    _max_channel_name: int
    __max_msg_id_ln: ClassVar[int] = 10

    def __init__(
        self,
        *args: Any,
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        log_fmt: Annotated[
            Optional[str],
            deprecated(
                "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                "Pass a pre-configured `logger` instead."
            ),
        ] = EMPTY,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *args,
            logger=logger,
            # TODO: generate unique logger names to not share between brokers
            default_logger=get_broker_logger(
                name="redis",
                default_context={
                    "channel": "",
                },
                message_id_ln=self.__max_msg_id_ln,
            ),
            log_level=log_level,
            log_fmt=log_fmt,
            **kwargs,
        )
        self._max_channel_name = 4

    def get_fmt(self) -> str:
        return (
            "%(asctime)s %(levelname)-8s - "
            f"%(channel)-{self._max_channel_name}s | "
            f"%(message_id)-{self.__max_msg_id_ln}s "
            "- %(message)s"
        )

    def _setup_log_context(
        self,
        *,
        channel: Optional[str] = None,
    ) -> None:
        self._max_channel_name = max((self._max_channel_name, len(channel or "")))
