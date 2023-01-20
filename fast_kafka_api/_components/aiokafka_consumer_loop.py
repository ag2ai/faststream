# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/001_ConsumerLoop.ipynb.

# %% auto 0
__all__ = ['logger', 'kafka_server_url', 'kafka_server_port', 'aiokafka_config', 'sanitize_kafka_config',
           'aiokafka_consumer_loop']

# %% ../../nbs/001_ConsumerLoop.ipynb 1
import asyncio
from asyncio import iscoroutinefunction  # do not use the version from inspect
from datetime import datetime, timedelta
from os import environ
from typing import *

from fastcore.meta import delegates
import anyio
import asyncer
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord, TopicPartition
from anyio.streams.memory import MemoryObjectReceiveStream
from pydantic import BaseModel, Field, HttpUrl, NonNegativeInt

from .logger import get_logger

# %% ../../nbs/001_ConsumerLoop.ipynb 6
logger = get_logger(__name__)

# %% ../../nbs/001_ConsumerLoop.ipynb 8
kafka_server_url = (
    environ["KAFKA_HOSTNAME"] if "KAFKA_HOSTNAME" in environ else "localhost"
)

kafka_server_port = environ["KAFKA_PORT"] if "KAFKA_PORT" in environ else "9092"

aiokafka_config = {
    "bootstrap_servers": f"{kafka_server_url}:{kafka_server_port}",
}

# %% ../../nbs/001_ConsumerLoop.ipynb 11
def _create_safe_callback(
    callback: Callable[[BaseModel], Awaitable[None]]
) -> Callable[[BaseModel], Awaitable[None]]:
    """
    Wraps an async callback into a safe callback that catches any Exception and loggs them as warnings

    Params:
        callback: async callable that will be wrapped into a safe callback

    Returns:
        Wrapped callback into a safe callback that handles exceptions
    """

    async def _safe_callback(
        msg: BaseModel,
        callback: Callable[[BaseModel], Awaitable[None]] = callback,
    ) -> None:
        try:
            #                         logger.debug(f"process_msgs(): awaiting '{callback}({msg})'")
            await callback(msg)
        except Exception as e:
            logger.warning(
                f"_safe_callback(): exception caugth {e.__repr__()} while awaiting '{callback}({msg})'"
            )

    return _safe_callback

# %% ../../nbs/001_ConsumerLoop.ipynb 14
def _prepare_callback(
    callback: Union[Callable[[BaseModel], None], Callable[[BaseModel], Awaitable[None]]]
) -> Callable[[BaseModel], Awaitable[None]]:
    """
    Prepares a callback to be used in the consumer loop.
        1. If callback is sync, asyncify it
        2. Wrap the callback into a safe callback for exception handling

    Params:
        callback: async callable that will be prepared for use in consumer

    Returns:
        Prepared callback
    """
    callback: Callable[[BaseModel], Awaitable[None]] = (
        callback if iscoroutinefunction(callback) else asyncer.asyncify(callback)
    )
    return _create_safe_callback(callback)

# %% ../../nbs/001_ConsumerLoop.ipynb 17
async def _stream_msgs(
    msgs: Dict[TopicPartition, bytes],
    send_stream: anyio.streams.memory.MemoryObjectSendStream[Any],
) -> None:
    """
    Decodes and streams the message and topic to the send_stream.

    Params:
        msgs:
        send_stream:
    """
    for topic_partition, topic_msgs in msgs.items():
        topic = topic_partition.topic
        try:
            await send_stream.send(topic_msgs)
        except Exception as e:
            logger.warning(
                f"_stream_msgs(): Unexpected exception '{e.__repr__()}' caught and ignored for topic='{topic_partition.topic}', partition='{topic_partition.partition}' and messages: {topic_msgs}"
            )


def _decode_streamed_msgs(
    msgs: List[ConsumerRecord], msg_type: BaseModel
) -> List[BaseModel]:
    decoded_msgs = [msg_type.parse_raw(msg.value.decode("utf-8")) for msg in msgs]
    return decoded_msgs

# %% ../../nbs/001_ConsumerLoop.ipynb 22
async def _streamed_records(receive_stream):
    async for records_per_topic in receive_stream:
        for records in records_per_topic:
            for record in records:
                yield record


@delegates(AIOKafkaConsumer.getmany)
async def _aiokafka_consumer_loop(  # type: ignore
    consumer: AIOKafkaConsumer,
    *,
    topic: str,
    callback: Callable[[BaseModel], Union[None, Awaitable[None]]],
    max_buffer_size: int = 100_000,
    msg_type: Type[BaseModel],
    is_shutting_down_f: Callable[[], bool],
    **kwargs,
) -> None:
    """
    Consumer loop for infinite pooling of the AIOKafka consumer for new messages. Calls consumer.getmany()
    and after the consumer return messages or times out, messages are decoded and streamed to defined callback.

    Params:
        callbacks: Dict of callbacks mapped to their respective topics
        timeout_ms: Time to timeut the getmany request by the consumer
        max_buffer_size: Maximum number of unconsumed messages in the callback buffer
        msg_types: Dict of message types mapped to their respective topics
        is_shutting_down_f: Function for controlling the shutdown of consumer loop
    """

    prepared_callback = _prepare_callback(callback)

    async def process_message_callback(
        receive_stream: MemoryObjectReceiveStream[Any],
        callback=prepared_callback,
        msg_type=msg_type,
        topic=topic,
    ) -> None:
        async with receive_stream:
            try:
                async for record in _streamed_records(receive_stream):
                    try:
                        msg = record.value
                        decoded_msg = msg_type.parse_raw(msg.decode("utf-8"))
                        await callback(decoded_msg)
                    except Exception as e:
                        logger.warning(
                            f"process_message_callback(): Unexpected exception '{e.__repr__()}' caught and ignored for topic='{topic}' and message: {msg}"
                        )
            except Exception as e:
                logger.warning(
                    f"process_message_callback(): Unexpected exception '{e.__repr__()}' caught and ignored for topic='{topic}'"
                )

    send_stream, receive_stream = anyio.create_memory_object_stream(
        max_buffer_size=max_buffer_size
    )

    async with anyio.create_task_group() as tg:
        tg.start_soon(process_message_callback, receive_stream)
        async with send_stream:
            while not is_shutting_down_f():
                msgs = await consumer.getmany(**kwargs)
                try:
                    await send_stream.send(msgs.values())
                except Exception as e:
                    logger.warning(
                        f"_aiokafka_consumer_loop(): Unexpected exception '{e}' caught and ignored for messages: {msgs}"
                    )

# %% ../../nbs/001_ConsumerLoop.ipynb 27
def sanitize_kafka_config(**kwargs):
    """Sanitize Kafka config"""
    return {k: "*" * len(v) if "pass" in k.lower() else v for k, v in kwargs.items()}

# %% ../../nbs/001_ConsumerLoop.ipynb 29
@delegates(AIOKafkaConsumer)
@delegates(_aiokafka_consumer_loop, keep=True)
async def aiokafka_consumer_loop(  # type: ignore
    topic: str,
    *,
    timeout_ms: int = 100,
    max_buffer_size: int = 100_000,
    callback: Dict[str, Callable[[BaseModel], Union[None, Awaitable[None]]]],
    msg_type: Dict[str, Type[BaseModel]],
    is_shutting_down_f: Callable[[], bool],
    **kwargs,
) -> None:
    """Consumer loop for infinite pooling of the AIOKafka consumer for new messages. Creates and starts AIOKafkaConsumer
    and runs _aio_kafka_consumer loop fo infinite poling of the consumer for new messages.

    Args:
        topic: name of the topic to subscribe to
        callback: callback function to be called after decoding and parsing a consumed message
        timeout_ms: Time to timeut the getmany request by the consumer
        max_buffer_size: Maximum number of unconsumed messages in the callback buffer
        msg_type: Type with `parse_json` method used for parsing a decoded message
        is_shutting_down_f: Function for controlling the shutdown of consumer loop
    """
    logger.info(f"aiokafka_consumer_loop() starting...")
    try:
        consumer = AIOKafkaConsumer(
            **kwargs,
        )
        logger.info(
            f"aiokafka_consumer_loop(): Consumer created using the following parameters: {sanitize_kafka_config(**kwargs)}"
        )

        await consumer.start()
        logger.info("aiokafka_consumer_loop(): Consumer started.")
        consumer.subscribe([topic])
        logger.info("aiokafka_consumer_loop(): Consumer subscribed.")

        try:
            await _aiokafka_consumer_loop(
                consumer=consumer,
                topic=topic,
                max_buffer_size=max_buffer_size,
                timeout_ms=timeout_ms,
                callback=callback,
                msg_type=msg_type,
                is_shutting_down_f=is_shutting_down_f,
            )
        finally:
            await consumer.stop()
            logger.info(f"aiokafka_consumer_loop(): Consumer stopped.")
            logger.info(f"aiokafka_consumer_loop() finished.")
    except Exception as e:
        logger.error(
            f"aiokafka_consumer_loop(): unexpected exception raised: '{e.__repr__()}'"
        )
        raise e
