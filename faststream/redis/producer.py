from typing import Any, Optional, Union
from uuid import uuid4

from redis.asyncio.client import PubSub, Redis

from faststream.broker.parsers import encode_message, resolve_custom_func
from faststream.broker.types import (
    AsyncCustomDecoder,
    AsyncCustomParser,
    AsyncDecoder,
    AsyncParser,
)
from faststream.exceptions import WRONG_PUBLISH_ARGS
from faststream.redis.message import (
    BatchMessage,
    OneMessage,
    PubSubMessage,
    RedisMessage,
)
from faststream.redis.parser import DATA_KEY, RawMessage, RedisParser
from faststream.redis.schemas import INCORRECT_SETUP_MSG
from faststream.types import AnyDict, DecodedMessage, SendableMessage
from faststream.utils.functions import timeout_scope


class RedisFastProducer:
    _connection: Redis[Any]
    _decoder: AsyncDecoder[Any]
    _parser: AsyncParser[PubSubMessage, Any]

    def __init__(
        self,
        connection: Redis[Any],
        parser: Optional[
            AsyncCustomParser[Union[OneMessage, BatchMessage], RedisMessage]
        ],
        decoder: Optional[AsyncCustomDecoder[RedisMessage]],
    ):
        self._connection = connection
        self._parser = resolve_custom_func(parser, RedisParser.parse_message)  # type: ignore[assignment,type-var,arg-type]
        self._decoder = resolve_custom_func(decoder, RedisParser.decode_message)  # type: ignore[assignment,type-var]

    async def publish(
        self,
        message: SendableMessage,
        channel: Optional[str] = None,
        reply_to: str = "",
        headers: Optional[AnyDict] = None,
        correlation_id: Optional[str] = None,
        *,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        rpc: bool = False,
        rpc_timeout: Optional[float] = 30.0,
        raise_timeout: bool = False,
    ) -> Optional[DecodedMessage]:
        if not any((channel, list, stream)):
            raise ValueError(INCORRECT_SETUP_MSG)

        psub: Optional[PubSub] = None
        if rpc is True:
            if reply_to:
                raise WRONG_PUBLISH_ARGS

            reply_to = str(uuid4())
            psub = self._connection.pubsub()  # type: ignore[assignment]
            await psub.subscribe(reply_to)  # type: ignore[union-attr]

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
            await self._connection.xadd(stream, {DATA_KEY: msg})
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

    async def publish_batch(
        self,
        *msgs: SendableMessage,
        list: str,
    ) -> None:
        batch = (encode_message(msg)[0] for msg in msgs)
        await self._connection.rpush(list, *batch)
