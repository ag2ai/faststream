from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any

import aiokafka

from faststream.__about__ import SERVICE_NAME
from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream._internal.utils.data import filter_by_dict
from faststream.kafka.helpers import AdminService
from faststream.kafka.publisher.producer import (
    AioKafkaFastProducer,
    FakeAioKafkaFastProducer,
)
from faststream.kafka.schemas.params import ConsumerConnectionParams


@dataclass(kw_only=True)
class KafkaBrokerConfig(BrokerConfig):
    producer: "AioKafkaFastProducer" = field(default_factory=FakeAioKafkaFastProducer)
    builder: Callable[..., aiokafka.AIOKafkaConsumer] = lambda: None

    client_id: str | None = SERVICE_NAME
    client_rack: str | None = None
    consumer_only: bool = False
    allow_auto_create_topics: bool = True
    admin: AdminService = field(default_factory=AdminService)

    @property
    def admin_client(self) -> "aiokafka.admin.client.AIOKafkaAdminClient":
        return self.admin.client

    async def connect(self, **connection_kwargs: Any) -> "None":
        # In consumer-only mode the broker neither produces messages nor needs
        # admin permissions, so skip creating those clients to allow callers
        # to use credentials scoped to read-only ACLs.
        if not self.consumer_only:
            producer = aiokafka.AIOKafkaProducer(**connection_kwargs)
            await self.producer.connect(
                producer,
                serializer=self.fd_config._serializer,
                codec=self.broker_codec or DefaultCodec(),
            )
            await self.admin.connect(**connection_kwargs)

        consumer_options, _ = filter_by_dict(
            ConsumerConnectionParams,
            connection_kwargs,
        )
        # client_rack is consumer-only, so it is not part of connection_kwargs
        # (which is also used to build the producer); inject it here when set.
        if self.client_rack is not None:
            consumer_options["client_rack"] = self.client_rack
        self.builder = partial(aiokafka.AIOKafkaConsumer, **consumer_options)

    async def disconnect(self) -> "None":
        await self.admin.disconnect()

        if not self.consumer_only:
            await self.producer.disconnect()
