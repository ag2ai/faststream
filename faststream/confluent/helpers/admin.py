from dataclasses import dataclass
from typing import TYPE_CHECKING

from confluent_kafka.admin import AdminClient, NewTopic

if TYPE_CHECKING:
    from .config import ConfluentFastConfig


@dataclass
class CreateResult:
    topic: str
    error: Exception | None


class AdminService:
    def __init__(self) -> None:
        self.admin_client = None

    async def connect(self, config: "ConfluentFastConfig") -> None:
        if self.admin_client is None:
            self.admin_client = AdminClient(config.admin_config)

    async def disconnect(self) -> None:
        self.admin_client = None

    def create_topics(self, topics: list[str]) -> list[CreateResult]:
        assert self.admin_client is not None

        create_result = self.admin_client.create_topics(
            [
                NewTopic(topic, num_partitions=1, replication_factor=1)
                for topic in topics
            ],
        )

        final_results = []
        for topic, f in create_result.items():
            try:
                f.result()

            except Exception as e:
                if "TOPIC_ALREADY_EXISTS" not in str(e):
                    result = CreateResult(topic, e)
                else:
                    result = CreateResult(topic, None)

            else:
                result = CreateResult(topic, None)

            final_results.append(result)

        return final_results
