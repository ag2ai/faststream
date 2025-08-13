"""GCP Pub/Sub publisher configuration."""

from dataclasses import dataclass


@dataclass
class PublisherConfig:
    """Configuration for GCP Pub/Sub publisher settings."""

    max_messages: int = 100
    max_bytes: int = 1024 * 1024  # 1MB
    max_latency: float = 0.01  # 10ms
