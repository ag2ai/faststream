from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import aiokafka.admin
from aiokafka.errors import TopicAlreadyExistsError, for_code

from faststream._internal.utils.data import filter_by_dict
from faststream.exceptions import IncorrectState
from faststream.kafka.schemas.params import AdminClientConnectionParams

if TYPE_CHECKING:
    from aiokafka.admin.client import AIOKafkaAdminClient

    from faststream.kafka.schemas import Topic


@dataclass
class CreateResult:
    topic: str
    error: Exception | None


class AdminService:
    def __init__(self) -> None:
        self.admin_client: AIOKafkaAdminClient | None = None

    async def connect(self, **connection_kwargs: Any) -> None:
        if self.admin_client is not None:
            return

        admin_options, _ = filter_by_dict(
            AdminClientConnectionParams,
            connection_kwargs,
        )
        self.admin_client = aiokafka.admin.client.AIOKafkaAdminClient(**admin_options)
        await self.admin_client.start()

    async def disconnect(self) -> None:
        if self.admin_client is not None:
            await self.admin_client.close()
            self.admin_client = None

    @property
    def client(self) -> "AIOKafkaAdminClient":
        if self.admin_client is None:
            msg = "Admin client is not initialized. Call connect() first."
            raise IncorrectState(msg)
        return self.admin_client

    async def create_topics(self, topics: list["Topic"]) -> list[CreateResult]:
        """Create topics through the admin client.

        Already-exists is success. Any other per-topic error is collected rather
        than raised, so a subscriber still starts against a topic the cluster
        refused to create.
        """
        if not topics:
            return []

        response = await self.client.create_topics(
            [topic.to_aiokafka() for topic in topics],
        )

        results = []
        for topic_error in response.to_object()["topic_errors"]:
            topic = topic_error["topic"]
            error_code = topic_error["error_code"]
            if not error_code:
                results.append(CreateResult(topic, None))
                continue

            error = for_code(error_code)(
                topic_error.get("error_message") or f"error code {error_code}",
            )
            if isinstance(error, TopicAlreadyExistsError):
                results.append(CreateResult(topic, None))
            else:
                results.append(CreateResult(topic, error))

        return results
