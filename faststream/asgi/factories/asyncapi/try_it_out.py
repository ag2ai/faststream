import json
from typing import TYPE_CHECKING, Any

from faststream._internal._compat import json_dumps
from faststream.asgi.factories.broker import (
    get_publish_param_name,
    test_broker_publish_context,
)
from faststream.asgi.response import AsgiResponse
from faststream.asgi.types import ASGIApp, Receive, Scope, Send

if TYPE_CHECKING:
    from faststream._internal.broker import BrokerUsecase


class TryItOutProcessor:
    """Process try-it-out requests: parse, validate, publish to real or test broker."""

    def __init__(self, broker: "BrokerUsecase[Any, Any]") -> None:
        self._broker = broker
        self._param_name = get_publish_param_name(broker)

    def parse_body(
        self, body_bytes: bytes
    ) -> tuple[AsgiResponse | None, dict[str, Any] | None]:
        """Parse JSON body. Returns (error_response, None) or (None, parsed_body)."""
        if not body_bytes:
            return None, {}
        try:
            body = json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            return _json_response({"error": f"Invalid JSON: {e}"}, 400), None
        if not isinstance(body, dict):
            return _json_response({"error": "Body must be a JSON object"}, 400), None
        return None, body

    async def process(self, body: dict[str, Any]) -> AsgiResponse:
        """Process parsed body: validate, dry-run or publish. Returns response."""
        channel_name = body.get("channelName")
        message_data = body.get("message", {})
        options = body.get("options", {})

        if not channel_name:
            return _json_response({"error": "Missing channelName"}, 400)

        payload = message_data.get("message", message_data)
        if isinstance(payload, dict):
            payload = {
                k: v
                for k, v in payload.items()
                if k not in {"operation_id", "operation_type"}
            }
        destination = channel_name.split(":")[0] if ":" in channel_name else channel_name
        publish_kwargs = {self._param_name: destination}

        use_real_broker = options.get("sendToRealBroker", False)

        try:
            if use_real_broker:
                # Publish to real broker (Kafka, RabbitMQ, etc.).
                await self._broker.publish(payload, **publish_kwargs)  # type: ignore[call-arg]
            else:
                # Publish via TestBroker (in-memory) for testing without real broker.
                with test_broker_publish_context(self._broker):
                    await self._broker.publish(payload, **publish_kwargs)  # type: ignore[call-arg]
        except Exception as e:
            return _json_response({"error": str(e)}, 500)

        mode = "real" if use_real_broker else "test"
        return _json_response({"status": "ok", "mode": mode}, 200)


def make_try_it_out_handler(
    broker: "BrokerUsecase[Any, Any]",
) -> ASGIApp:
    """Create POST handler for asyncapi-try-it-plugin to publish messages to broker."""
    processor = TryItOutProcessor(broker)

    async def try_it_out(scope: Scope, receive: Receive, send: Send) -> None:
        if scope.get("method") != "POST":
            response = AsgiResponse(b"Method Not Allowed", 405)
            await response(scope, receive, send)
            return

        body_bytes = b""
        while True:
            message = await receive()
            if message["type"] == "http.request":
                body_bytes += message.get("body", b"")
                if not message.get("more_body", False):
                    break

        error_response, body = processor.parse_body(body_bytes)
        if error_response is not None:
            await error_response(scope, receive, send)
            return
        assert body is not None  # When error_response is None, body is always a dict
        response = await processor.process(body)
        await response(scope, receive, send)

    return try_it_out


def _json_response(data: dict[str, Any], status_code: int = 200) -> AsgiResponse:
    body = json_dumps(data)
    if isinstance(body, str):
        body = body.encode("utf-8")
    return AsgiResponse(
        body,
        status_code,
        {"Content-Type": "application/json"},
    )
