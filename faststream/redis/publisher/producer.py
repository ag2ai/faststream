from typing import TYPE_CHECKING, Any, Optional

import anyio
from typing_extensions import override

from faststream.broker.publisher.proto import ProducerProto
from faststream.broker.utils import resolve_custom_func
from faststream.exceptions import WRONG_PUBLISH_ARGS, SetupError
from faststream.redis.message import DATA_KEY
from faststream.redis.parser import RawMessage, RedisPubSubParser
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.utils.functions import timeout_scope
from faststream.utils.nuid import NUID

if TYPE_CHECKING:
    from redis.asyncio.client import Pipeline, PubSub, Redis

    from faststream.broker.types import (
        AsyncCallable,
        CustomCallable,
    )
    from faststream.types import AnyDict, SendableMessage


class RedisFastProducer(ProducerProto):
    """A class to represent a Redis producer."""

    _connection: "Redis[bytes]"
    _decoder: "AsyncCallable"
    _parser: "AsyncCallable"

    def __init__(
        self,
        connection: "Redis[bytes]",
        parser: Optional["CustomCallable"],
        decoder: Optional["CustomCallable"],
    ) -> None:
        self._connection = connection

        default = RedisPubSubParser()
        self._parser = resolve_custom_func(
            parser,
            default.parse_message,
        )
        self._decoder = resolve_custom_func(
            decoder,
            default.decode_message,
        )

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage",
        *,
        correlation_id: str,
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
        headers: Optional["AnyDict"] = None,
        reply_to: str = "",
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> Optional[Any]:
        if not any((channel, list, stream)):
            raise SetupError(INCORRECT_SETUP_MSG)

        if pipeline is not None and rpc is True:
            raise RuntimeError(
                "You cannot use both rpc and pipeline arguments at the same time: "
                "select only one delivery mechanism."
            )

        psub: Optional[PubSub] = None
        if rpc:
            if reply_to:
                raise WRONG_PUBLISH_ARGS
            nuid = NUID()
            rpc_nuid = str(nuid.next(), "utf-8")
            reply_to = rpc_nuid
            psub = self._connection.pubsub()
            await psub.subscribe(reply_to)

        msg = RawMessage.encode(
            message=message,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
        )

        conn = pipeline or self._connection
        if channel is not None:
            await conn.publish(channel, msg)
        elif list is not None:
            await conn.rpush(list, msg)
        elif stream is not None:
            await conn.xadd(
                name=stream,
                fields={DATA_KEY: msg},
                maxlen=maxlen,
            )
        else:
            raise AssertionError("unreachable")

        if psub is None:
            return None

        else:
            m = None
            with timeout_scope(rpc_timeout, raise_timeout):
                # skip subscribe message
                await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=rpc_timeout or 0.0,
                )

                # get real response
                m = await psub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=rpc_timeout or 0.0,
                )

            await psub.unsubscribe()
            await psub.aclose()  # type: ignore[attr-defined]

            if m is None:
                if raise_timeout:
                    raise TimeoutError()
                else:
                    return None
            else:
                return await self._decoder(await self._parser(m))

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        *,
        correlation_id: str,
        channel: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
        headers: Optional["AnyDict"] = None,
        timeout: Optional[float] = 30.0,
    ) -> "Any":
        if not any((channel, list, stream)):
            raise SetupError(INCORRECT_SETUP_MSG)

        nuid = NUID()
        reply_to = str(nuid.next(), "utf-8")
        psub = self._connection.pubsub()
        await psub.subscribe(reply_to)

        msg = RawMessage.encode(
            message=message,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
        )

        if channel is not None:
            await self._connection.publish(channel, msg)
        elif list is not None:
            await self._connection.rpush(list, msg)
        elif stream is not None:
            await self._connection.xadd(
                name=stream,
                fields={DATA_KEY: msg},
                maxlen=maxlen,
            )
        else:
            raise AssertionError("unreachable")

        with anyio.fail_after(timeout) as scope:
            # skip subscribe message
            await psub.get_message(
                ignore_subscribe_messages=True,
                timeout=timeout or 0.0,
            )

            # get real response
            response_msg = await psub.get_message(
                ignore_subscribe_messages=True,
                timeout=timeout or 0.0,
            )

        await psub.unsubscribe()
        await psub.aclose()  # type: ignore[attr-defined]

        if scope.cancel_called:
            raise TimeoutError

        return response_msg

    async def publish_batch(
        self,
        *msgs: "SendableMessage",
        list: str,
        correlation_id: str,
        headers: Optional["AnyDict"] = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> None:
        batch = (
            RawMessage.encode(
                message=msg,
                correlation_id=correlation_id,
                reply_to=None,
                headers=headers,
            )
            for msg in msgs
        )
        conn = pipeline or self._connection
        await conn.rpush(list, *batch)
