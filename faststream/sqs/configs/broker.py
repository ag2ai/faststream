from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import IncorrectState
from faststream.sqs.publisher.producer import SQSFastProducer

if TYPE_CHECKING:
    from types_aiobotocore_sqs import SQSClient

    from faststream.sqs.schemas import SQSQueue


@dataclass(kw_only=True)
class SQSBrokerConfig(BrokerConfig):
    producer: "SQSFastProducer" = field(default_factory=SQSFastProducer)
    _client: "SQSClient | None" = field(default=None, init=False, repr=False)
    _queue_urls: dict[str, str] = field(default_factory=dict, init=False, repr=False)

    @property
    def client(self) -> "SQSClient":
        if self._client is None:
            msg = "SQS broker is not connected. Call connect() first."
            raise IncorrectState(msg)
        return self._client

    def connect(self, client: "SQSClient") -> None:
        self._client = client
        self.producer.connect(
            client,
            self.fd_config._serializer,
            codec=self.broker_codec or DefaultCodec(),
            queue_urls=self._queue_urls,
        )

    def disconnect(self) -> None:
        self._client = None
        self.producer.disconnect()

    async def get_queue_url(self, queue: str) -> str:
        if queue.startswith(("http://", "https://")):
            return queue
        if queue in self._queue_urls:
            return self._queue_urls[queue]
        resp = await self.client.get_queue_url(QueueName=queue)
        url = resp["QueueUrl"]
        self._queue_urls[queue] = url
        return url

    async def declare_queue(self, queue: "SQSQueue", *, name: str | None = None) -> str:
        """Create the queue if needed and cache its URL.

        ``name`` overrides ``queue.queue_name`` so a router prefix can be applied
        without mutating the queue object.
        """
        queue_name = name or queue.queue_name
        create_kwargs: dict[str, Any] = {
            "QueueName": queue_name,
            "Attributes": queue.to_attributes(),
        }
        if queue.tags:
            create_kwargs["tags"] = queue.tags
        resp = await self.client.create_queue(**create_kwargs)
        url = resp["QueueUrl"]
        self._queue_urls[queue_name] = url
        return url
