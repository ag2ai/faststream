# AUTOGENERATED! DO NOT EDIT! File to edit: ../../nbs/001_InMemoryBroker.ipynb.

# %% auto 0
__all__ = ['logger', 'KafkaRecord', 'KafkaPartition', 'KafkaTopic', 'ConsumerMetadata', 'create_consumer_record',
           'InMemoryBroker', 'InMemoryConsumer', 'InMemoryProducer']

# %% ../../nbs/001_InMemoryBroker.ipynb 1
import uuid
from collections import namedtuple
from dataclasses import dataclass
from contextlib import contextmanager
import inspect
import asyncio
import copy
import random

from typing import *
import fastkafka._application.app
import fastkafka._components.aiokafka_consumer_loop
import fastkafka._components.aiokafka_producer_manager
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord, TopicPartition, RecordMetadata

import fastkafka._application.app
from fastkafka._components.meta import (
    copy_func,
    patch,
    delegates,
    classcontextmanager,
    _get_default_kwargs_from_sig,
)
from .._components.logger import get_logger

# %% ../../nbs/001_InMemoryBroker.ipynb 3
logger = get_logger(__name__)

# %% ../../nbs/001_InMemoryBroker.ipynb 6
@dataclass
class KafkaRecord:
    topic: str = ""
    partition: int = 0
    key: Optional[bytes] = None
    value: bytes = b""
    offset: int = 0

# %% ../../nbs/001_InMemoryBroker.ipynb 7
class KafkaPartition:
    def __init__(self, *, partition: int, topic: str):
        self.partition = partition
        self.topic = topic
        self.messages: List[KafkaRecord] = list()

    def write(self, value: bytes, key: Optional[bytes] = None) -> KafkaRecord:
        record = KafkaRecord(
            topic=self.topic,
            partition=self.partition,
            value=value,
            key=key,
            offset=len(self.messages),
        )
        self.messages.append(record)
        return record

    def read(self, offset: int) -> List[KafkaRecord]:
        return self.messages[offset:]

# %% ../../nbs/001_InMemoryBroker.ipynb 11
class KafkaTopic:
    def __init__(self, topic: str, num_partitions: int = 1):
        self.topic = topic
        self.num_partitions = num_partitions
        self.partitions: List[KafkaPartition] = [
            KafkaPartition(topic=topic, partition=partition_index)
            for partition_index in range(num_partitions)
        ]

    def read(  # type: ignore
        self, partition: int, offset: int
    ) -> Dict[TopicPartition, List[KafkaRecord]]:
        topic_partition = TopicPartition(topic=self.topic, partition=partition)
        records = self.partitions[partition].read(offset)
        return {topic_partition: records}

    def write_with_partition(
        self,
        value: bytes,
        partition: int,
    ) -> KafkaRecord:
        return self.partitions[partition].write(value)

    def write_with_key(self, value: bytes, key: bytes) -> KafkaRecord:
        partition = int(hash(key)) % self.num_partitions
        return self.partitions[partition].write(value, key=key)

    def write(
        self,
        value: bytes,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
    ) -> KafkaRecord:
        if partition is not None:
            return self.write_with_partition(value, partition)

        if key is not None:
            return self.write_with_key(value, key)

        partition = random.randint(0, self.num_partitions - 1)  # nosec
        return self.write_with_partition(value, partition)

# %% ../../nbs/001_InMemoryBroker.ipynb 17
@dataclass
class ConsumerMetadata:
    topic: str
    offset: int

# %% ../../nbs/001_InMemoryBroker.ipynb 20
def create_consumer_record(topic: str, msg: bytes) -> ConsumerRecord:  # type: ignore
    record = ConsumerRecord(
        topic=topic,
        partition=0,
        offset=0,
        timestamp=0,
        timestamp_type=0,
        key=None,
        value=msg,
        checksum=0,
        serialized_key_size=0,
        serialized_value_size=0,
        headers=[],
    )
    return record

# %% ../../nbs/001_InMemoryBroker.ipynb 23
# InMemoryBroker
@classcontextmanager()
class InMemoryBroker:
    def __init__(self, topics: Set[str]):
        self.data: Dict[str, List[ConsumerRecord]] = {topic: list() for topic in topics}  # type: ignore
        self.consumers_metadata: Dict[uuid.UUID, List[ConsumerMetadata]] = {}
        self.is_started: bool = False

    def connect(self) -> uuid.UUID:
        return uuid.uuid4()

    def subscribe(
        self, actor_id: uuid.UUID, *, auto_offest_reset: str, topic: str
    ) -> None:
        consumer_metadata = self.consumers_metadata.get(actor_id, list())
        # todo: what if whatever?
        consumer_metadata.append(
            ConsumerMetadata(
                topic, len(self.data[topic]) if auto_offest_reset == "latest" else 0
            )
        )
        self.consumers_metadata[actor_id] = consumer_metadata

    def unsubscribe(self, actor_id: uuid.UUID) -> None:
        try:
            del self.consumers_metadata[actor_id]
        except KeyError:
            logger.warning(f"No subscription with {actor_id=} found!")

    def produce(  # type: ignore
        self, *, topic: str, msg: bytes, key: Optional[bytes] = None
    ) -> RecordMetadata:
        if topic in self.data:
            record = create_consumer_record(topic, msg)
            self.data[topic].append(record)
        else:
            # todo: log only once
            logger.warning(
                f"Topic {topic} is not available during auto-create initialization"
            )
        return RecordMetadata(
            topic=topic,
            partition=0,
            topic_partition=TopicPartition(topic=topic, partition=0),
            offset=0,
            timestamp=1680602752070,
            timestamp_type=0,
            log_start_offset=0,
        )

    def consume(  # type: ignore
        self, actor_id: uuid.UUID
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        msgs: Dict[TopicPartition, List[ConsumerRecord]] = {}  # type: ignore

        consumer_metadata = self.consumers_metadata[actor_id]

        for metadata in consumer_metadata:
            try:
                msgs[TopicPartition(metadata.topic, 0)] = self.data[metadata.topic][
                    metadata.offset :
                ]
                metadata.offset = len(self.data[metadata.topic])
            except KeyError:
                raise RuntimeError(
                    f"{metadata.topic=} not found, did you pass it to InMemoryBroker on init to be created?"
                )
        return msgs

    @contextmanager
    def lifecycle(self) -> Iterator["InMemoryBroker"]:
        raise NotImplementedError()

    async def _start(self) -> str:
        logger.info("InMemoryBroker._start() called")
        self.__enter__()  # type: ignore
        return "localbroker:0"

    async def _stop(self) -> None:
        logger.info("InMemoryBroker._stop() called")
        self.__exit__(None, None, None)  # type: ignore

# %% ../../nbs/001_InMemoryBroker.ipynb 28
# InMemoryConsumer
class InMemoryConsumer:
    def __init__(
        self,
        broker: InMemoryBroker,
    ) -> None:
        self.broker = broker
        self._id: Optional[uuid.UUID] = None
        self._auto_offset_reset: str = "latest"

    @delegates(AIOKafkaConsumer)
    def __call__(self, **kwargs: Any) -> "InMemoryConsumer":
        defaults = _get_default_kwargs_from_sig(InMemoryConsumer.__call__, **kwargs)
        consume_copy = InMemoryConsumer(self.broker)
        consume_copy._auto_offset_reset = defaults["auto_offset_reset"]
        return consume_copy

    @delegates(AIOKafkaConsumer.start)
    async def start(self, **kwargs: Any) -> None:
        pass

    @delegates(AIOKafkaConsumer.stop)
    async def stop(self, **kwargs: Any) -> None:
        pass

    @delegates(AIOKafkaConsumer.subscribe)
    def subscribe(self, topics: List[str], **kwargs: Any) -> None:
        raise NotImplementedError()

    @delegates(AIOKafkaConsumer.getmany)
    async def getmany(  # type: ignore
        self, **kwargs: Any
    ) -> Dict[TopicPartition, List[ConsumerRecord]]:
        raise NotImplementedError()

# %% ../../nbs/001_InMemoryBroker.ipynb 31
@patch
@delegates(AIOKafkaConsumer.start)
async def start(self: InMemoryConsumer, **kwargs: Any) -> None:
    logger.info("AIOKafkaConsumer patched start() called()")
    if self._id is not None:
        raise RuntimeError(
            "Consumer start() already called! Run consumer stop() before running start() again"
        )
    self._id = self.broker.connect()

# %% ../../nbs/001_InMemoryBroker.ipynb 34
@patch
@delegates(AIOKafkaConsumer.subscribe)
def subscribe(self: InMemoryConsumer, topics: List[str], **kwargs: Any) -> None:
    logger.info("AIOKafkaConsumer patched subscribe() called")
    if self._id is None:
        raise RuntimeError("Consumer start() not called! Run consumer start() first")
    logger.info(f"AIOKafkaConsumer.subscribe(), subscribing to: {topics}")
    for topic in topics:
        self.broker.subscribe(
            self._id, topic=topic, auto_offest_reset=self._auto_offset_reset
        )

# %% ../../nbs/001_InMemoryBroker.ipynb 37
@patch
@delegates(AIOKafkaConsumer.stop)
async def stop(self: InMemoryConsumer, **kwargs: Any) -> None:
    logger.info("AIOKafkaConsumer patched stop() called")
    if self._id is None:
        raise RuntimeError("Consumer start() not called! Run consumer start() first")
    self.broker.unsubscribe(self._id)

# %% ../../nbs/001_InMemoryBroker.ipynb 40
@patch
@delegates(AIOKafkaConsumer.getmany)
async def getmany(  # type: ignore
    self: InMemoryConsumer, **kwargs: Any
) -> Dict[TopicPartition, List[ConsumerRecord]]:
    return self.broker.consume(self._id)  # type: ignore

# %% ../../nbs/001_InMemoryBroker.ipynb 43
class InMemoryProducer:
    def __init__(self, broker: InMemoryBroker, **kwargs: Any) -> None:
        self.broker = broker
        self.id: Optional[uuid.UUID] = None

    @delegates(AIOKafkaProducer)
    def __call__(self, **kwargs: Any) -> "InMemoryProducer":
        return InMemoryProducer(self.broker)

    @delegates(AIOKafkaProducer.start)
    async def start(self, **kwargs: Any) -> None:
        raise NotImplementedError()

    @delegates(AIOKafkaProducer.stop)
    async def stop(self, **kwargs: Any) -> None:
        raise NotImplementedError()

    @delegates(AIOKafkaProducer.send)
    async def send(  # type: ignore
        self: AIOKafkaProducer,
        topic: str,
        msg: bytes,
        key: Optional[bytes] = None,
        **kwargs: Any,
    ):
        raise NotImplementedError()

# %% ../../nbs/001_InMemoryBroker.ipynb 45
@patch  # type: ignore
@delegates(AIOKafkaProducer.start)
async def start(self: InMemoryProducer, **kwargs: Any) -> None:
    logger.info("AIOKafkaProducer patched start() called()")
    if self.id is not None:
        raise RuntimeError(
            "Producer start() already called! Run producer stop() before running start() again"
        )
    self.id = self.broker.connect()

# %% ../../nbs/001_InMemoryBroker.ipynb 48
@patch  # type: ignore
@delegates(AIOKafkaProducer.stop)
async def stop(self: InMemoryProducer, **kwargs: Any) -> None:
    logger.info("AIOKafkaProducer patched stop() called")
    if self.id is None:
        raise RuntimeError("Producer start() not called! Run producer start() first")

# %% ../../nbs/001_InMemoryBroker.ipynb 51
@patch
@delegates(AIOKafkaProducer.send)
async def send(  # type: ignore
    self: InMemoryProducer,
    topic: str,
    msg: bytes,
    key: Optional[bytes] = None,
    **kwargs: Any,
):  # asyncio.Task[ConsumerRecord]
    if self.id is None:
        raise RuntimeError("Producer start() not called! Run producer start() first")
    record = self.broker.produce(topic=topic, msg=msg, key=key)

    async def _f(record: ConsumerRecord = record) -> ConsumerRecord:  # type: ignore
        return record

    return asyncio.create_task(_f())

# %% ../../nbs/001_InMemoryBroker.ipynb 54
@patch
@contextmanager
def lifecycle(self: InMemoryBroker) -> Iterator[InMemoryBroker]:
    logger.info(
        "InMemoryBroker._patch_consumers_and_producers(): Patching consumers and producers!"
    )
    try:
        logger.info("InMemoryBroker starting")

        old_consumer_app = fastkafka._application.app.AIOKafkaConsumer
        old_producer_app = fastkafka._application.app.AIOKafkaProducer
        old_consumer_loop = (
            fastkafka._components.aiokafka_consumer_loop.AIOKafkaConsumer
        )
        old_producer_manager = (
            fastkafka._components.aiokafka_producer_manager.AIOKafkaProducer
        )

        fastkafka._application.app.AIOKafkaConsumer = InMemoryConsumer(self)
        fastkafka._application.app.AIOKafkaProducer = InMemoryProducer(self)
        fastkafka._components.aiokafka_consumer_loop.AIOKafkaConsumer = (
            InMemoryConsumer(self)
        )
        fastkafka._components.aiokafka_producer_manager.AIOKafkaProducer = (
            InMemoryProducer(self)
        )

        self.is_started = True
        yield self
    finally:
        logger.info("InMemoryBroker stopping")

        fastkafka._application.app.AIOKafkaConsumer = old_consumer_app
        fastkafka._application.app.AIOKafkaProducer = old_producer_app
        fastkafka._components.aiokafka_consumer_loop.AIOKafkaConsumer = (
            old_consumer_loop
        )
        fastkafka._components.aiokafka_producer_manager.AIOKafkaProducer = (
            old_producer_manager
        )

        self.is_started = False
