from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterator
    from types import TracebackType

    from faststream.mq import MQBroker


@dataclass(frozen=True)
class MQAdminConfig:
    queue_manager: str = "QM1"
    channel: str = "DEV.ADMIN.SVRCONN"
    host: str = "127.0.0.1"
    port: int = 1414
    username: str = "admin"
    password: str = "password"

    @property
    def conn_name(self) -> str:
        return f"{self.host}({self.port})"


def _load_ibmmq() -> Any:
    import ibmmq as mq

    return mq


@contextmanager
def admin_pcf(config: MQAdminConfig) -> "Iterator[tuple[Any, Any]]":
    mq = _load_ibmmq()
    qmgr = mq.QueueManager(None)
    pcf = None

    try:
        qmgr.connect_tcp_client(
            config.queue_manager,
            mq.CD(),
            config.channel,
            config.conn_name,
            config.username,
            config.password,
        )
        pcf = mq.PCFExecute(qmgr, response_wait_interval=30000)
        yield mq, pcf

    finally:
        if pcf is not None:
            disconnect = getattr(pcf, "disconnect", None)
            if disconnect is not None:
                disconnect()

        if qmgr.is_connected:
            qmgr.disconnect()


def ensure_queue(queue_name: str, config: MQAdminConfig) -> bool:
    with admin_pcf(config) as (mq, pcf):
        args = {
            mq.CMQC.MQCA_Q_NAME: queue_name.encode(),
            mq.CMQC.MQIA_Q_TYPE: mq.CMQC.MQQT_LOCAL,
        }

        try:
            pcf.MQCMD_INQUIRE_Q(args)
        except mq.MQMIError as e:
            if (
                e.comp == mq.CMQC.MQCC_FAILED
                and e.reason == mq.CMQC.MQRC_UNKNOWN_OBJECT_NAME
            ):
                pcf.MQCMD_CREATE_Q(args)
                return True
            raise

    return False


def delete_queue(queue_name: str, config: MQAdminConfig) -> None:
    with admin_pcf(config) as (mq, pcf):
        args = {
            mq.CMQC.MQCA_Q_NAME: queue_name.encode(),
            mq.CMQCFC.MQIACF_PURGE: mq.CMQCFC.MQPO_YES,
        }

        try:
            pcf.MQCMD_DELETE_Q(args)
        except mq.MQMIError as e:
            if (
                e.comp == mq.CMQC.MQCC_FAILED
                and e.reason == mq.CMQC.MQRC_UNKNOWN_OBJECT_NAME
            ):
                return
            raise


class ManagedMQBroker:
    def __init__(self, broker: "MQBroker", admin_config: MQAdminConfig) -> None:
        self._broker = broker
        self._admin_config = admin_config
        self._created_queues: set[str] = set()
        self._prepared = False

    def __getattr__(self, name: str) -> Any:
        return getattr(self._broker, name)

    async def __aenter__(self) -> "ManagedMQBroker":
        await self._broker.connect()
        if not self._prepared:
            self._prepare_queues()
            self._prepared = True
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: "TracebackType | None",
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        if not self._prepared:
            self._prepare_queues()
            self._prepared = True

        await self._broker.start()

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: "TracebackType | None" = None,
    ) -> None:
        try:
            await self._broker.stop(exc_type, exc_val, exc_tb)
        finally:
            for queue_name in sorted(self._created_queues):
                delete_queue(queue_name, self._admin_config)

            self._created_queues.clear()
            self._prepared = False

    def _prepare_queues(self) -> None:
        queue_names = {subscriber.routing() for subscriber in self._broker.subscribers}
        queue_names.update(publisher.routing() for publisher in self._broker.publishers)

        for queue_name in sorted(queue_names):
            if ensure_queue(queue_name, self._admin_config):
                self._created_queues.add(queue_name)
