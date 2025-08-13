"""Pytest fixtures and configuration for GCP Pub/Sub tests."""

import uuid
from collections.abc import AsyncGenerator
from dataclasses import dataclass

import pytest

from faststream.gcppubsub import GCPPubSubRouter


@dataclass
class GCPPubSubSettings:
    """GCP Pub/Sub test settings."""

    project_id: str = "test-project"
    emulator_host: str = "localhost:8681"  # Default emulator port
    subscription_prefix: str = "test-sub"
    topic_prefix: str = "test-topic"


@pytest.fixture(scope="session")
def gcppubsub_settings() -> GCPPubSubSettings:
    """Session-level settings for GCP Pub/Sub tests."""
    return GCPPubSubSettings()


@pytest.fixture()
def topic() -> str:
    """Generate unique topic name for test isolation."""
    return f"test-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def subscription() -> str:
    """Generate unique subscription name for test isolation."""
    return f"test-sub-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def queue(subscription: str) -> str:
    """Alias for subscription to match base test patterns."""
    return subscription


@pytest.fixture()
def router() -> GCPPubSubRouter:
    """Create clean router instance for testing."""
    return GCPPubSubRouter()


@pytest.fixture()
def response_topic() -> str:
    """Generate unique response topic name for request-response testing."""
    return f"response-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture()
def fake_producer_cls():
    """Provide the fake producer class for test client testing."""
    from faststream.gcppubsub.testing import FakeGCPPubSubProducer

    return FakeGCPPubSubProducer


@pytest.fixture()
async def emulator_broker(gcppubsub_settings: GCPPubSubSettings) -> AsyncGenerator:
    """Create broker configured for emulator testing."""
    from faststream.gcppubsub import GCPPubSubBroker

    broker = GCPPubSubBroker(
        project_id=gcppubsub_settings.project_id,
        # Configure for emulator - will be expanded as emulator support is added
    )

    yield broker
    await broker.stop()
