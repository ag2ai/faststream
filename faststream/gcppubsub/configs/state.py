"""GCP Pub/Sub connection state management."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from aiohttp import ClientSession
    from gcloud.aio.pubsub import PublisherClient, SubscriberClient


@dataclass
class ConnectionState:
    """Manages the connection state for GCP Pub/Sub clients."""

    session: Optional["ClientSession"] = None
    publisher: Optional["PublisherClient"] = None
    subscriber: Optional["SubscriberClient"] = None
    owns_session: bool = False

    async def close(self) -> None:
        """Close all connections."""
        if self.owns_session and self.session:
            await self.session.close()

        self.session = None
        self.publisher = None
        self.subscriber = None
