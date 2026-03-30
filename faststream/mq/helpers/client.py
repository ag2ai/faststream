from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import json
import os
from typing import TYPE_CHECKING, Any, cast

from faststream._internal.utils.functions import run_in_executor
from faststream.exceptions import INSTALL_FASTSTREAM_MQ
from faststream.message import encode_message
from faststream.mq.helpers.ids import mq_id_to_bytes, try_parse_mq_id
from faststream.mq.message import MQRawMessage
from faststream.mq.response import MQPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto


def _load_ibmmq() -> Any:
    try:
        os.environ.setdefault("MQIPY_NOOTEL", "true")
        import ibmmq as mq
    except ImportError as e:  # pragma: no cover - depends on optional dependency
        raise ImportError(INSTALL_FASTSTREAM_MQ) from e

    otel_functions = getattr(mq, "OTelFunctions", None)
    if otel_functions is not None:
        otel_functions.disc = None
        otel_functions.open = None
        otel_functions.close = None
        otel_functions.put_trace_before = None
        otel_functions.put_trace_after = None
        otel_functions.get_trace_before = None
        otel_functions.get_trace_after = None

    return mq


@dataclass(kw_only=True)
class MQConnectionConfig:
    queue_manager: str
    channel: str
    conn_name: str
    username: str | None = None
    password: str | None = None
    reply_model_queue: str = "DEV.APP.MODEL.QUEUE"
    wait_interval: float = 1.0
    use_ssl: bool = False
    ssl_context: Any = None


class AsyncMQConnection:
    def __init__(self, *, connection_config: MQConnectionConfig) -> None:
        self.connection_config = connection_config
        self._executor: ThreadPoolExecutor | None = None
        self._qmgr: Any = None
        self._consumer_queue: Any = None
        self._consumer_queue_name: str = ""

    async def connect(self) -> None:
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=1)

        await run_in_executor(self._executor, self._connect_sync)

    def _connect_sync(self) -> None:
        if self._qmgr is not None:
            return

        if (
            self.connection_config.use_ssl
            or self.connection_config.ssl_context is not None
        ):
            msg = "SSL-enabled IBM MQ connections are not supported yet."
            raise NotImplementedError(msg)

        mq = _load_ibmmq()

        qmgr = mq.QueueManager(None)
        qmgr.connect_tcp_client(
            self.connection_config.queue_manager,
            mq.CD(),
            self.connection_config.channel,
            self.connection_config.conn_name,
            self.connection_config.username,
            self.connection_config.password,
        )
        self._qmgr = qmgr

    async def disconnect(self) -> None:
        if self._executor is None:
            return

        try:
            await run_in_executor(self._executor, self._disconnect_sync)
        finally:
            self._executor.shutdown(wait=False)
            self._executor = None

    def _disconnect_sync(self) -> None:
        if self._consumer_queue is not None:
            try:
                self._consumer_queue.close()
            finally:
                self._consumer_queue = None
                self._consumer_queue_name = ""

        if self._qmgr is not None:
            try:
                self._qmgr.disconnect()
            finally:
                self._qmgr = None

    async def ping(self, timeout: float | None = None) -> bool:
        if self._executor is None:
            return False

        try:
            return await run_in_executor(self._executor, self._ping_sync)
        except Exception:
            return False

    def _ping_sync(self) -> bool:
        if self._qmgr is None:
            return False
        self._qmgr.get_name()
        return True

    async def commit(self) -> None:
        assert self._executor is not None, "Connection is not started yet."
        await run_in_executor(self._executor, self._commit_sync)

    def _commit_sync(self) -> None:
        assert self._qmgr is not None
        self._qmgr.commit()

    async def backout(self) -> None:
        assert self._executor is not None, "Connection is not started yet."
        await run_in_executor(self._executor, self._backout_sync)

    def _backout_sync(self) -> None:
        assert self._qmgr is not None
        self._qmgr.backout()

    async def start_consumer(self, queue_name: str) -> None:
        assert self._executor is not None, "Connection is not started yet."
        await run_in_executor(self._executor, self._start_consumer_sync, queue_name)

    def _start_consumer_sync(self, queue_name: str) -> None:
        mq = _load_ibmmq()

        if self._consumer_queue is not None:
            self._consumer_queue.close()

        assert self._qmgr is not None
        self._consumer_queue = mq.Queue(
            self._qmgr,
            queue_name,
            mq.CMQC.MQOO_INPUT_AS_Q_DEF,
        )
        self._consumer_queue_name = queue_name

    async def stop_consumer(self) -> None:
        if self._executor is None:
            return

        await run_in_executor(self._executor, self._stop_consumer_sync)

    def _stop_consumer_sync(self) -> None:
        if self._consumer_queue is not None:
            self._consumer_queue.close()
            self._consumer_queue = None
            self._consumer_queue_name = ""

    async def get_message(self, *, timeout: float) -> MQRawMessage | None:
        assert self._executor is not None, "Connection is not started yet."
        return await run_in_executor(self._executor, self._get_message_sync, timeout)

    def _get_message_sync(self, timeout: float) -> MQRawMessage | None:
        mq = _load_ibmmq()
        assert self._qmgr is not None
        assert self._consumer_queue is not None

        gmo = mq.GMO(Version=mq.CMQC.MQGMO_CURRENT_VERSION)
        gmo.Options = (
            mq.CMQC.MQGMO_WAIT
            | mq.CMQC.MQGMO_SYNCPOINT
            | mq.CMQC.MQGMO_PROPERTIES_IN_HANDLE
        )
        gmo.WaitInterval = _to_wait_interval(timeout)

        msg_handle = mq.MessageHandle(self._qmgr)
        gmo.MsgHandle = msg_handle.msg_handle

        md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
        body: bytes | None = None

        try:
            body = cast(bytes, self._consumer_queue.get(None, md, gmo))
        except mq.MQMIError as e:
            if e.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
                msg_handle.dlt()
                return None
            msg_handle.dlt()
            raise

        try:
            headers = _read_headers(msg_handle)
        finally:
            msg_handle.dlt()

        assert body is not None
        return _build_raw_message(
            body=body,
            md=md,
            headers=headers,
            queue_name=self._consumer_queue_name,
            connection=self,
        )

    async def publish(
        self,
        cmd: MQPublishCommand,
        *,
        serializer: "SerializerProto | None",
    ) -> None:
        assert self._executor is not None, "Connection is not started yet."
        await run_in_executor(self._executor, self._publish_sync, cmd, serializer)

    def _publish_sync(
        self,
        cmd: MQPublishCommand,
        serializer: "SerializerProto | None",
    ) -> None:
        mq = _load_ibmmq()
        assert self._qmgr is not None

        body, content_type = encode_message(cmd.body, serializer)
        headers = _headers_to_publish(cmd, content_type)

        queue = mq.Queue(self._qmgr, cmd.destination, mq.CMQC.MQOO_OUTPUT)
        msg_handle = None

        try:
            md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
            if cmd.reply_to:
                md.ReplyToQ = cmd.reply_to
            if cmd.reply_to_qmgr:
                md.ReplyToQMgr = cmd.reply_to_qmgr
            if cmd.priority is not None:
                md.Priority = cmd.priority
            if cmd.persistence is not None:
                md.Persistence = (
                    mq.CMQC.MQPER_PERSISTENT
                    if cmd.persistence
                    else mq.CMQC.MQPER_NOT_PERSISTENT
                )
            if cmd.expiry is not None:
                md.Expiry = cmd.expiry

            if cmd.message_id is not None:
                md.MsgId = mq_id_to_bytes(cmd.message_id, field_name="message_id")

            if cmd.native_correlation_id is not None:
                md.CorrelId = cmd.native_correlation_id
            elif cmd.correlation_id is not None:
                md.CorrelId = mq_id_to_bytes(
                    cmd.correlation_id,
                    field_name="correlation_id",
                )

            pmo = mq.PMO(Version=mq.CMQC.MQPMO_VERSION_3)
            if headers:
                msg_handle = mq.MessageHandle(self._qmgr)
                for key, value in headers.items():
                    msg_handle.properties.set(
                        _property_name_from_header(key),
                        _normalize_property_value(value),
                    )
                pmo.OriginalMsgHandle = msg_handle.msg_handle

            queue.put(body, md, pmo)

        finally:
            queue.close()
            if msg_handle is not None:
                msg_handle.dlt()

    async def request(
        self,
        cmd: MQPublishCommand,
        *,
        serializer: "SerializerProto | None",
    ) -> MQRawMessage:
        assert self._executor is not None, "Connection is not started yet."
        return await run_in_executor(self._executor, self._request_sync, cmd, serializer)

    def _request_sync(
        self,
        cmd: MQPublishCommand,
        serializer: "SerializerProto | None",
    ) -> MQRawMessage:
        mq = _load_ibmmq()
        assert self._qmgr is not None

        dyn_od = mq.OD()
        dyn_od.ObjectName = self.connection_config.reply_model_queue
        dyn_od.DynamicQName = "FASTSTREAM.REPLY.*"
        reply_queue = mq.Queue(self._qmgr, dyn_od, mq.CMQC.MQOO_INPUT_EXCLUSIVE)
        reply_queue_name = _mq_str(dyn_od.ObjectName)

        try:
            body, content_type = encode_message(cmd.body, serializer)
            headers = _headers_to_publish(cmd, content_type)

            queue = mq.Queue(self._qmgr, cmd.destination, mq.CMQC.MQOO_OUTPUT)
            put_handle = None

            try:
                md = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
                md.ReplyToQ = reply_queue_name
                md.ReplyToQMgr = self.connection_config.queue_manager
                if cmd.priority is not None:
                    md.Priority = cmd.priority
                if cmd.persistence is not None:
                    md.Persistence = (
                        mq.CMQC.MQPER_PERSISTENT
                        if cmd.persistence
                        else mq.CMQC.MQPER_NOT_PERSISTENT
                    )
                if cmd.expiry is not None:
                    md.Expiry = cmd.expiry

                if cmd.message_id is not None:
                    md.MsgId = mq_id_to_bytes(cmd.message_id, field_name="message_id")

                if cmd.correlation_id is not None:
                    md.CorrelId = mq_id_to_bytes(
                        cmd.correlation_id,
                        field_name="correlation_id",
                    )

                pmo = mq.PMO(Version=mq.CMQC.MQPMO_VERSION_3)
                if headers:
                    put_handle = mq.MessageHandle(self._qmgr)
                    for key, value in headers.items():
                        put_handle.properties.set(
                            _property_name_from_header(key),
                            _normalize_property_value(value),
                        )
                    pmo.OriginalMsgHandle = put_handle.msg_handle

                queue.put(body, md, pmo)
                request_message_id = bytes(md.MsgId)

            finally:
                queue.close()
                if put_handle is not None:
                    put_handle.dlt()

            gmo = mq.GMO(Version=mq.CMQC.MQGMO_CURRENT_VERSION)
            gmo.Options = (
                mq.CMQC.MQGMO_WAIT
                | mq.CMQC.MQGMO_NO_SYNCPOINT
                | mq.CMQC.MQGMO_PROPERTIES_IN_HANDLE
            )
            gmo.MatchOptions = mq.CMQC.MQMO_MATCH_CORREL_ID
            gmo.WaitInterval = _to_wait_interval(cmd.timeout)

            get_handle = mq.MessageHandle(self._qmgr)
            gmo.MsgHandle = get_handle.msg_handle

            md_get = mq.MD(Version=mq.CMQC.MQMD_CURRENT_VERSION)
            md_get.CorrelId = request_message_id

            try:
                reply_body = cast(bytes, reply_queue.get(None, md_get, gmo))
            except mq.MQMIError as e:
                if e.reason == mq.CMQC.MQRC_NO_MSG_AVAILABLE:
                    get_handle.dlt()
                    raise TimeoutError from e
                get_handle.dlt()
                raise

            try:
                headers = _read_headers(get_handle)
            finally:
                get_handle.dlt()

            return _build_raw_message(
                body=reply_body,
                md=md_get,
                headers=headers,
                queue_name=reply_queue_name,
                connection=None,
            )

        finally:
            reply_queue.close()


def _headers_to_publish(
    cmd: MQPublishCommand,
    content_type: str | None,
) -> dict[str, Any]:
    headers = dict(cmd.headers)
    if content_type:
        headers.setdefault("content-type", content_type)
    if cmd.message_type:
        headers.setdefault("message_type", cmd.message_type)
    return headers


def _normalize_property_value(value: Any) -> str | bytes | bool | int | float | None:
    if value is None or isinstance(value, (str, bytes, bool, int, float)):
        return value
    return json.dumps(value)


def _read_headers(msg_handle: Any) -> dict[str, Any]:
    mq = _load_ibmmq()
    headers: dict[str, Any] = {}
    options = mq.CMQC.MQIMPO_INQ_FIRST

    while True:
        try:
            value, property_name = msg_handle.properties.get(
                "usr.%",
                impo_options=options,
            )
        except mq.MQMIError as e:
            if e.reason == mq.CMQC.MQRC_PROPERTY_NOT_AVAILABLE:
                break
            raise

        headers[
            _header_name_from_property(_strip_property_prefix(_mq_str(property_name)))
        ] = value
        options = mq.CMQC.MQIMPO_INQ_NEXT

    return headers


def _strip_property_prefix(name: str) -> str:
    if name.startswith("usr."):
        return name[4:]
    return name


def _property_name_from_header(name: str) -> str:
    return f"usr.{name.replace('-', '_dash_')}"


def _header_name_from_property(name: str) -> str:
    return name.replace("_dash_", "-")


def _build_raw_message(
    *,
    body: bytes,
    md: Any,
    headers: dict[str, Any],
    queue_name: str,
    connection: AsyncMQConnection | None,
) -> MQRawMessage:
    headers = dict(headers)
    header_message_id = try_parse_mq_id(headers.pop("message_id", None))
    header_correlation_id = try_parse_mq_id(headers.pop("correlation_id", None))
    content_type = cast(str | None, headers.get("content-type"))

    native_message_id = _to_bytes(getattr(md, "MsgId", None))
    native_correlation_id = _to_bytes(getattr(md, "CorrelId", None))

    return MQRawMessage(
        body=body,
        queue=queue_name,
        headers=headers,
        reply_to=_mq_str(getattr(md, "ReplyToQ", b"")),
        reply_to_qmgr=_mq_str(getattr(md, "ReplyToQMgr", b"")),
        content_type=content_type,
        correlation_id=try_parse_mq_id(native_correlation_id) or header_correlation_id,
        message_id=try_parse_mq_id(native_message_id) or header_message_id,
        native_message_id=native_message_id,
        native_correlation_id=native_correlation_id,
        priority=cast(int | None, getattr(md, "Priority", None)),
        persistence=_decode_persistence(getattr(md, "Persistence", None)),
        expiry=cast(int | None, getattr(md, "Expiry", None)),
        metadata=md,
        connection=connection,
    )


def _decode_persistence(value: Any) -> bool | None:
    mq = _load_ibmmq()
    if value == mq.CMQC.MQPER_PERSISTENT:
        return True
    if value == mq.CMQC.MQPER_NOT_PERSISTENT:
        return False
    return None


def _to_bytes(value: Any) -> bytes | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    return None


def _mq_str(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode().strip()
    if isinstance(value, str):
        return value.strip()
    return ""


def _to_wait_interval(timeout: float) -> int:
    milliseconds = int(timeout * 1000)
    return max(milliseconds, 1)
