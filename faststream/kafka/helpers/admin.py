from dataclasses import dataclass
from typing import TYPE_CHECKING

from aiokafka.errors import TopicAlreadyExistsError, for_code

if TYPE_CHECKING:
    from aiokafka.admin.client import AIOKafkaAdminClient

    from faststream.kafka.schemas import Topic


@dataclass
class CreateResult:
    topic: str
    error: Exception | None


async def create_topics(
    admin_client: "AIOKafkaAdminClient",
    topics: list["Topic"],
) -> list[CreateResult]:
    """Create topics through the admin client.

    Already-exists is success. Any other per-topic error is collected rather
    than raised, so a subscriber still starts against a topic the cluster
    refused to create.
    """
    if not topics:
        return []

    response = await admin_client.create_topics(
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
