"""Tests for GCP Pub/Sub response with attributes."""

import pytest

from faststream.gcp import GCPBroker, GCPResponse, MessageAttributes
from faststream.gcp.testing import TestGCPBroker


class TestGCPResponseAttributes:
    """Test GCPResponse with attributes."""

    @pytest.mark.asyncio()
    async def test_gcp_response_with_attributes(self) -> None:
        """Test returning GCPResponse with attributes from handler."""
        broker = GCPBroker(project_id="test-project")
        input_received = {}
        output_received = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def process_message(msg: str, attrs: MessageAttributes) -> GCPResponse:
            input_received.update({"msg": msg, "attrs": attrs})

            # Return response with custom attributes
            return GCPResponse(
                body=f"processed: {msg}",
                attributes={
                    "request_id": attrs.get("request_id", "unknown"),
                    "processed_by": "handler",
                    "status": "success",
                    "original_priority": attrs.get("priority", "normal"),
                },
                ordering_key="processed-order",
            )

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            output_received.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "input message",
                topic="input-topic",
                attributes={"request_id": "req-123", "priority": "high"},
            )

        # Check input processing
        assert input_received["msg"] == "input message"
        assert input_received["attrs"]["request_id"] == "req-123"
        assert input_received["attrs"]["priority"] == "high"

        # Check output with response attributes
        assert output_received["message"] == "processed: input message"
        assert output_received["attributes"]["request_id"] == "req-123"
        assert output_received["attributes"]["processed_by"] == "handler"
        assert output_received["attributes"]["status"] == "success"
        assert output_received["attributes"]["original_priority"] == "high"

    @pytest.mark.asyncio()
    async def test_gcp_response_attribute_forwarding(self) -> None:
        """Test forwarding and modifying attributes through response."""
        broker = GCPBroker(project_id="test-project")
        chain_results = []

        @broker.subscriber("step1-sub", topic="step1-topic")
        @broker.publisher("step2-topic")
        async def step1_handler(msg: dict, attrs: MessageAttributes) -> GCPResponse:
            chain_results.append(f"step1: {msg['data']}")

            # Forward some attributes and add new ones
            new_attrs = {
                "trace_id": attrs.get("trace_id", "unknown"),
                "step": "1",
                "processed_at": "2024-01-01T12:00:00Z",
                "handler": "step1_handler",
            }

            return GCPResponse(
                body={"data": f"step1-{msg['data']}", "count": msg.get("count", 0) + 1},
                attributes=new_attrs,
            )

        @broker.subscriber("step2-sub", topic="step2-topic")
        @broker.publisher("step3-topic")
        async def step2_handler(msg: dict, attrs: MessageAttributes) -> GCPResponse:
            chain_results.append(f"step2: {msg['data']}")

            # Continue the chain with modified attributes
            new_attrs = {
                "trace_id": attrs.get("trace_id"),
                "step": "2",
                "previous_step": attrs.get("step"),
                "processed_at": "2024-01-01T12:01:00Z",
                "handler": "step2_handler",
            }

            return GCPResponse(
                body={"data": f"step2-{msg['data']}", "count": msg["count"] + 1},
                attributes=new_attrs,
            )

        @broker.subscriber("step3-sub", topic="step3-topic")
        async def step3_handler(msg: dict, attrs: MessageAttributes) -> None:
            chain_results.append(f"step3: {msg['data']} (final count: {msg['count']})")
            chain_results.append(f"trace: {attrs.get('trace_id')}")
            chain_results.append(
                f"chain: step {attrs.get('previous_step')} -> step {attrs.get('step')}"
            )

        async with TestGCPBroker(broker) as br:
            await br.publish(
                {"data": "initial", "count": 0},
                topic="step1-topic",
                attributes={"trace_id": "trace-abc123", "source": "test"},
            )

        assert len(chain_results) == 5
        assert chain_results[0] == "step1: initial"
        assert chain_results[1] == "step2: step1-initial"
        assert chain_results[2] == "step3: step2-step1-initial (final count: 2)"
        assert chain_results[3] == "trace: trace-abc123"
        assert chain_results[4] == "chain: step 1 -> step 2"

    @pytest.mark.asyncio()
    async def test_gcp_response_without_attributes(self) -> None:
        """Test GCPResponse without attributes still works."""
        broker = GCPBroker(project_id="test-project")
        received_data = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def simple_handler(msg: str) -> GCPResponse:
            # Response without attributes
            return GCPResponse(body=f"simple: {msg}")

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            received_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish("test message", topic="input-topic")

        assert received_data["message"] == "simple: test message"
        assert isinstance(received_data["attributes"], dict)

    @pytest.mark.asyncio()
    async def test_gcp_response_correlation_id_propagation(self) -> None:
        """Test correlation ID propagation through GCPResponse."""
        broker = GCPBroker(project_id="test-project")
        received_data = {}

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def correlation_handler(msg: str, attrs: MessageAttributes) -> GCPResponse:
            # Extract correlation ID and propagate it
            correlation_id = attrs.get("correlation_id")

            return GCPResponse(
                body=f"correlated: {msg}",
                attributes={"original_correlation": correlation_id},
                correlation_id=correlation_id,  # Propagate correlation ID
            )

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            received_data.update({
                "message": msg,
                "attributes": attrs,
            })

        async with TestGCPBroker(broker) as br:
            await br.publish(
                "correlated message",
                topic="input-topic",
                correlation_id="corr-123",
            )

        assert received_data["message"] == "correlated: correlated message"
        assert "correlation_id" in received_data["attributes"]
        assert received_data["attributes"]["original_correlation"] is not None

    @pytest.mark.asyncio()
    async def test_mixed_response_types(self) -> None:
        """Test mixing GCPResponse with regular returns."""
        broker = GCPBroker(project_id="test-project")
        received_messages = []

        @broker.subscriber("input-sub", topic="input-topic")
        @broker.publisher("output-topic")
        async def mixed_handler(msg: str, attrs: MessageAttributes) -> str | GCPResponse:
            priority = attrs.get("priority", "normal")

            if priority == "high":
                # High priority messages get special treatment with attributes
                return GCPResponse(
                    body=f"HIGH: {msg}",
                    attributes={
                        "priority": "high",
                        "processed_by": "priority_handler",
                        "timestamp": "2024-01-01T12:00:00Z",
                    },
                    ordering_key="high-priority",
                )
            # Normal messages get simple response
            return f"NORMAL: {msg}"

        @broker.subscriber("output-sub", topic="output-topic")
        async def output_handler(msg: str, attrs: MessageAttributes) -> None:
            received_messages.append({
                "message": msg,
                "attributes": dict(attrs),
            })

        async with TestGCPBroker(broker) as br:
            # Send normal priority message
            await br.publish(
                "normal message",
                topic="input-topic",
                attributes={"priority": "normal"},
            )

            # Send high priority message
            await br.publish(
                "urgent message",
                topic="input-topic",
                attributes={"priority": "high"},
            )

        assert len(received_messages) == 2

        # Check normal message (no special attributes)
        normal_msg = received_messages[0]
        assert normal_msg["message"] == "NORMAL: normal message"

        # Check high priority message (with special attributes)
        high_msg = received_messages[1]
        assert high_msg["message"] == "HIGH: urgent message"
        assert high_msg["attributes"]["priority"] == "high"
        assert high_msg["attributes"]["processed_by"] == "priority_handler"
        assert high_msg["attributes"]["timestamp"] == "2024-01-01T12:00:00Z"
