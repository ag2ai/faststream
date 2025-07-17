from collections.abc import Callable
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Optional

import aiokafka
import aiokafka.admin

from faststream.__about__ import SERVICE_NAME
from faststream._internal.configs import BrokerConfig
from faststream._internal.utils.data import filter_by_dict
from faststream.exceptions import IncorrectState
from faststream.kafka.publisher.producer import (
    AioKafkaFastProducer,
    FakeAioKafkaFastProducer,
)
from faststream.kafka.schemas.params import (
    AdminClientConnectionParams,
    ConsumerConnectionParams,
)


@dataclass(kw_only=True)
class KafkaBrokerConfig(BrokerConfig):
    producer: "AioKafkaFastProducer" = field(default_factory=FakeAioKafkaFastProducer)
    builder: Callable[..., aiokafka.AIOKafkaConsumer] = lambda: None

    client_id: str | None = SERVICE_NAME

    _admin_client: Optional["aiokafka.admin.client.AIOKafkaAdminClient"] = None

    @property
    def admin_client(self) -> "aiokafka.admin.client.AIOKafkaAdminClient":
        if self._admin_client is None:
            msg = "Admin client is not initialized. Call connect() first."
            raise IncorrectState(msg)

        return self._admin_client

    async def connect(self, **connection_kwargs: Any) -> "None":
        producer = aiokafka.AIOKafkaProducer(**connection_kwargs)
        await self.producer.connect(producer, serializer=self.fd_config._serializer)

        admin_options, _ = filter_by_dict(
            AdminClientConnectionParams,
            connection_kwargs,
        )

        self._admin_client = aiokafka.admin.client.AIOKafkaAdminClient(**admin_options)
        await self._admin_client.start()

        consumer_options, _ = filter_by_dict(
            ConsumerConnectionParams,
            connection_kwargs,
        )
        self.builder = partial(aiokafka.AIOKafkaConsumer, **consumer_options)

    async def disconnect(self) -> "None":
        if self._admin_client is not None:
            await self._admin_client.close()
            self._admin_client = None

        await self.producer.disconnect()
