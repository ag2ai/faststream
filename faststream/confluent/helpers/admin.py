from dataclasses import dataclass
from typing import Optional

from confluent_kafka.admin import AdminClient, NewTopic

from .config import ConfluentFastConfig


@dataclass
class CreateResult:
    topic: str
    error: Optional[Exception]


class AdminService:
    def __init__(self, config: ConfluentFastConfig) -> None:
        self.config = config
        self.admin_client = None

    async def start(self) -> None:
        if self.admin_client is None:
            self.admin_client = AdminClient(self.config.admin_config)

    async def close(self) -> None:
        self.admin_client = None

    def create_topics(self, topics: str) -> list[CreateResult]:
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
