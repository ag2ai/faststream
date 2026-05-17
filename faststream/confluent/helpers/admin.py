from dataclasses import dataclass
from typing import TYPE_CHECKING

from confluent_kafka.admin import (  # type: ignore[attr-defined]
    AdminClient,
    NewTopic,
)

from .client import _LazyLoggerProxy

if TYPE_CHECKING:
    from faststream._internal.logger import LoggerState

    from .config import ConfluentFastConfig


@dataclass
class CreateResult:
    topic: str
    error: Exception | None


class AdminService:
    def __init__(self) -> None:
        self.admin_client: AdminClient | None = None

    async def connect(
        self,
        config: "ConfluentFastConfig",
        logger: "LoggerState | None" = None,
    ) -> None:
        if self.admin_client is None:
            admin_config = config.admin_config
            if logger is not None:
                self.admin_client = AdminClient(
                    admin_config,
                    logger=_LazyLoggerProxy(logger),
                )
            else:
                self.admin_client = AdminClient(admin_config)

    async def disconnect(self) -> None:
        self.admin_client = None

    @property
    def client(self) -> AdminClient:
        assert self.admin_client is not None, (
            "Admin client was not connected. Please, connect the broker first."
        )
        return self.admin_client

    def create_topics(self, topics: list[str]) -> list[CreateResult]:
        create_result = self.client.create_topics(
            [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics],
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
