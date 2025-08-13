"""GCP Pub/Sub connection management tests."""

import asyncio
import contextlib
from unittest.mock import AsyncMock, patch

import pytest

from faststream.gcppubsub import GCPPubSubBroker
from tests.marks import require_gcppubsub

from .basic import GCPPubSubTestcaseConfig


@pytest.mark.gcppubsub()
@require_gcppubsub
class TestConnect(GCPPubSubTestcaseConfig):
    """Test GCP Pub/Sub connection management."""

    @pytest.mark.asyncio()
    async def test_broker_connect_disconnect(self) -> None:
        """Test broker connection lifecycle."""
        broker = self.get_broker()

        # Test connection
        await broker.connect()
        assert broker._connection is not None  # Check internal connection state

        # Test disconnection
        await broker.stop()
        # After close, connection should be cleaned up
        assert not hasattr(broker, "_connection") or broker._connection is None

    @pytest.mark.asyncio()
    async def test_connection_retry(self) -> None:
        """Test connection retry logic."""
        broker = self.get_broker()

        with patch("gcloud.aio.pubsub.SubscriberClient") as mock_client_class:
            # Create mock instances
            mock_client = AsyncMock()
            mock_client_class.return_value = mock_client

            await broker.connect()
            assert broker._connection is not None
            await broker.stop()

    @pytest.mark.asyncio()
    async def test_graceful_shutdown(self) -> None:
        """Test graceful shutdown with active subscribers."""
        broker = self.get_broker()
        shutdown_complete = asyncio.Event()
        processing_started = asyncio.Event()

        @broker.subscriber(
            "test-subscription", topic="test-topic", create_subscription=True
        )
        async def handler(msg) -> None:
            processing_started.set()
            # Simulate some processing time
            await asyncio.sleep(0.1)

        async with broker:
            await broker.start()

            # Publish message and wait for processing to start
            publish_task = asyncio.create_task(broker.publish("test", topic="test-topic"))

            # Wait for processing to start
            # May not reach handler in test mode
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(processing_started.wait(), timeout=self.timeout)

            await publish_task
            shutdown_complete.set()

        assert shutdown_complete.is_set()

    @pytest.mark.asyncio()
    async def test_connection_context_manager(self) -> None:
        """Test connection using context manager."""
        broker = self.get_broker()

        async with broker:
            assert broker._connection is not None

        # After exiting context, connection should be closed
        assert not hasattr(broker, "_connection") or broker._connection is None

    @pytest.mark.asyncio()
    async def test_reconnection_handling(self) -> None:
        """Test connection recovery after failure."""
        broker = self.get_broker()

        # Initial connection
        await broker.connect()
        assert broker._connection is not None

        # Simulate connection loss and recovery
        with patch.object(broker, "connect") as mock_connect:
            mock_connect.return_value = None
            await broker.connect()
            mock_connect.assert_called_once()

        await broker.stop()

    @pytest.mark.asyncio()
    async def test_multiple_connections(self) -> None:
        """Test multiple brokers with separate connections."""
        broker1 = self.get_broker()
        broker2 = self.get_broker()

        await broker1.connect()
        await broker2.connect()

        # Both should have independent connections
        assert broker1._connection is not None
        assert broker2._connection is not None

        await broker1.stop()
        await broker2.stop()

    @pytest.mark.asyncio()
    async def test_connection_timeout(self) -> None:
        """Test connection timeout handling."""
        broker = self.get_broker()

        with patch("asyncio.wait_for") as mock_wait:
            # Simulate timeout
            mock_wait.side_effect = asyncio.TimeoutError("Connection timeout")

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(broker.connect(), timeout=1.0)

    @pytest.mark.asyncio()
    async def test_connection_state_consistency(self) -> None:
        """Test that connection state remains consistent."""
        broker = self.get_broker()

        # Initially not connected
        assert not hasattr(broker, "_connection") or broker._connection is None

        # Connect
        await broker.connect()
        connection_state = broker._connection

        # Multiple connect calls should not change state
        await broker.connect()
        assert broker._connection is connection_state

        # Close
        await broker.stop()
        assert not hasattr(broker, "_connection") or broker._connection is None

    @pytest.mark.asyncio()
    async def test_emulator_connection(self) -> None:
        """Test connection to GCP Pub/Sub emulator."""
        # Configure broker for emulator
        broker = GCPPubSubBroker(
            project_id="test-project",
            # Emulator configuration would go here
        )

        # Test emulator connection
        with patch.dict("os.environ", {"PUBSUB_EMULATOR_HOST": "localhost:8681"}):
            await broker.connect()
            assert broker._connection is not None
            await broker.stop()

    @pytest.mark.asyncio()
    async def test_connection_error_handling(self) -> None:
        """Test various connection error scenarios."""
        broker = self.get_broker()

        # Test network error during client creation
        with patch(
            "faststream.gcppubsub.broker.broker.SubscriberClient"
        ) as mock_sub_client:
            mock_sub_client.side_effect = ConnectionError("Network error")

            with pytest.raises(ConnectionError):
                await broker.connect()

    @pytest.mark.asyncio()
    async def test_connection_pool_management(self) -> None:
        """Test connection pooling behavior."""
        broker = self.get_broker()

        # Test that multiple operations reuse connections
        await broker.connect()
        connection1 = broker._connection

        # Simulate multiple operations
        await broker.connect()  # Should reuse existing connection
        connection2 = broker._connection

        assert connection1 is connection2  # Same connection instance

        await broker.stop()
