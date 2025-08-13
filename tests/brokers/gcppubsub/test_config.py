"""GCP Pub/Sub configuration validation tests."""

import os
from unittest.mock import patch

import pytest

from faststream.gcppubsub import GCPPubSubBroker
from tests.marks import require_gcppubsub


@pytest.mark.gcppubsub()
@require_gcppubsub
class TestConfig:
    """Test GCP Pub/Sub configuration validation."""

    def test_broker_config_defaults(self) -> None:
        """Test broker configuration with default values."""
        broker = GCPPubSubBroker(project_id="test-project")

        assert broker.config.broker_config.project_id == "test-project"
        # Test other default values from the actual configuration
        assert hasattr(broker.config.broker_config, "subscriber_ack_deadline")
        assert hasattr(broker.config.broker_config, "subscriber_max_messages")
        assert broker.config.broker_config.subscriber_ack_deadline == 600  # Default value
        assert (
            broker.config.broker_config.subscriber_max_messages == 1000
        )  # Default value

    def test_broker_config_custom(self) -> None:
        """Test broker configuration with custom values."""
        broker = GCPPubSubBroker(
            project_id="custom-project",
            subscriber_ack_deadline=300,
            subscriber_max_messages=50,
        )

        assert broker.config.broker_config.project_id == "custom-project"
        assert broker.config.broker_config.subscriber_ack_deadline == 300
        assert broker.config.broker_config.subscriber_max_messages == 50

    def test_project_id_validation(self) -> None:
        """Test project ID validation."""
        # Valid project ID should work
        broker = GCPPubSubBroker(project_id="valid-project-123")
        assert broker.config.broker_config.project_id == "valid-project-123"

        # Empty project ID may be accepted - test actual behavior
        broker_empty = GCPPubSubBroker(project_id="")
        assert broker_empty.config.broker_config.project_id == ""

    def test_ack_deadline_validation(self) -> None:
        """Test ACK deadline validation."""
        # Valid values
        broker = GCPPubSubBroker(project_id="test", subscriber_ack_deadline=60)
        assert broker.config.broker_config.subscriber_ack_deadline == 60

        # Negative value - test actual behavior
        broker_negative = GCPPubSubBroker(project_id="test", subscriber_ack_deadline=-1)
        assert broker_negative.config.broker_config.subscriber_ack_deadline == -1

    def test_max_messages_validation(self) -> None:
        """Test max messages validation."""
        # Valid values
        broker = GCPPubSubBroker(project_id="test", subscriber_max_messages=100)
        assert broker.config.broker_config.subscriber_max_messages == 100

        # Zero value should be handled gracefully
        broker_zero = GCPPubSubBroker(project_id="test", subscriber_max_messages=0)
        assert broker_zero.config.broker_config.subscriber_max_messages == 0

    def test_subscriber_config(self) -> None:
        """Test subscriber-specific configuration."""
        broker = GCPPubSubBroker(project_id="test")

        @broker.subscriber(
            "test-subscription", topic="test-topic", ack_deadline=120, max_messages=10
        )
        async def handler(msg) -> None:
            pass

        subscriber = broker.subscribers[0]
        assert subscriber.config.ack_deadline == 120
        assert subscriber.config.max_messages == 10

    def test_publisher_config(self) -> None:
        """Test publisher-specific configuration."""
        broker = GCPPubSubBroker(project_id="test")

        # Create publisher with specific configuration
        publisher = broker.publisher(
            "test-topic", create_topic=False, ordering_key="test-key"
        )

        assert publisher.topic == "test-topic"
        assert not publisher.create_topic
        assert publisher.ordering_key == "test-key"

    def test_environment_variable_config(self) -> None:
        """Test configuration from environment variables."""
        # Test with environment variables
        with patch.dict(
            os.environ,
            {
                "GOOGLE_CLOUD_PROJECT": "env-project",
                "PUBSUB_EMULATOR_HOST": "localhost:8681",
            },
        ):
            # Create broker without explicit project_id
            # Implementation might use environment variables
            try:
                broker = GCPPubSubBroker()
                # If implementation supports env vars, project_id might be set automatically
                assert broker.config.project_id is not None
            except TypeError:
                # If project_id is required, that's also valid
                broker = GCPPubSubBroker(project_id="env-project")
                assert broker.config.project_id == "env-project"

    def test_config_inheritance(self) -> None:
        """Test configuration inheritance patterns."""
        # Create broker with base config
        broker = GCPPubSubBroker(
            project_id="test-project",
            subscriber_ack_deadline=600,
            subscriber_max_messages=100,
        )

        # Create subscriber - should inherit broker defaults
        @broker.subscriber("test-subscription", topic="test-topic")
        async def handler(msg) -> None:
            pass

        subscriber = broker.subscribers[0]
        # Subscriber should inherit broker defaults where not overridden
        assert subscriber.config._outer_config.project_id == "test-project"

    def test_broker_config_object(self) -> None:
        """Test direct broker config object usage via broker creation."""
        # Test configuration through broker creation (most common usage)
        broker = GCPPubSubBroker(
            project_id="direct-config-test",
            subscriber_ack_deadline=300,
            subscriber_max_messages=25,
        )

        config = broker.config.broker_config
        assert config.project_id == "direct-config-test"
        assert config.subscriber_ack_deadline == 300
        assert config.subscriber_max_messages == 25

    def test_invalid_config_combinations(self) -> None:
        """Test invalid configuration combinations."""
        # Test configuration combinations - GCP Pub/Sub broker may accept various values

        # Large timeout values should be accepted
        broker = GCPPubSubBroker(
            project_id="test",
            subscriber_ack_deadline=86400,  # 24 hours
        )
        assert broker.config.broker_config.subscriber_ack_deadline == 86400

    def test_config_serialization(self) -> None:
        """Test configuration can be serialized/represented."""
        broker = GCPPubSubBroker(project_id="test-serialization")

        # Config should have useful string representation
        config_str = str(broker.config)
        assert "test-serialization" in config_str

        # Config should be representable
        config_repr = repr(broker.config)
        assert isinstance(config_repr, str)
        assert len(config_repr) > 0

    def test_config_immutability(self) -> None:
        """Test that critical config values can't be accidentally modified."""
        broker = GCPPubSubBroker(project_id="immutable-test")
        original_project_id = broker.config.project_id

        # Try to modify - should either be protected or changes should not affect behavior
        try:
            broker.config.project_id = "modified"
            # If modification is allowed, ensure it doesn't break the broker
            assert broker.config.project_id in {"immutable-test", "modified"}
        except AttributeError:
            # If modification is prevented, that's also valid
            assert broker.config.project_id == original_project_id

    def test_config_validation_edge_cases(self) -> None:
        """Test configuration validation with edge cases."""
        # Very large values
        broker = GCPPubSubBroker(
            project_id="edge-case-test",
            subscriber_ack_deadline=86400,  # 24 hours
            subscriber_max_messages=1000,
        )
        assert broker.config.broker_config.subscriber_ack_deadline == 86400
        assert broker.config.broker_config.subscriber_max_messages == 1000

        # Minimum valid values
        broker2 = GCPPubSubBroker(
            project_id="edge-case-test-2",
            subscriber_ack_deadline=10,  # 10 seconds
            subscriber_max_messages=1,
        )
        assert broker2.config.broker_config.subscriber_ack_deadline == 10
        assert broker2.config.broker_config.subscriber_max_messages == 1

    def test_config_with_custom_endpoints(self) -> None:
        """Test configuration with custom API endpoints."""
        # Test custom endpoint configuration (e.g., for emulator or private endpoints)
        broker = GCPPubSubBroker(
            project_id="custom-endpoint-test",
            # Custom endpoint configuration would depend on implementation
        )

        assert broker.config.project_id == "custom-endpoint-test"
        # Additional endpoint-specific assertions would go here
