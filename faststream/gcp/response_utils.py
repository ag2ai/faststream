"""GCP Pub/Sub response utilities."""

from typing import Any

from faststream.response.response import Response
from faststream.response.utils import ensure_response as base_ensure_response

from .response import GCPResponse
from .response_types import ResponseAttributes, ResponseOrderingKey


def ensure_gcp_response(response: Response | tuple[Any, ...] | Any) -> Response:
    """Convert handler return value to a Response object.

    Handles:
    - GCPResponse objects: returned as-is
    - Response objects: returned as-is
    - tuple containing message body and GCP-specific type markers:
      - ResponseAttributes: message attributes
      - ResponseOrderingKey: ordering key for message ordering
    - Any other value: converted to basic Response

    The tuple must contain a message body and at least one GCP-specific type marker.
    Items can be in any order - they are identified by type.

    Examples:
        return "message", ResponseAttributes({"key": "value"})
        return "message", ResponseOrderingKey("user-123")
        return ResponseOrderingKey("key"), "message", ResponseAttributes({"k": "v"})
        return {"data": "value"}, ResponseAttributes({"meta": "data"})

    Args:
        response: Handler return value

    Returns:
        Response object suitable for publishing
    """
    # Already a Response object
    if isinstance(response, Response):
        return response

    # Handle tuple returns by inspecting types
    if isinstance(response, tuple) and len(response) >= 2:
        # Find each component by type
        message_body = None
        attributes = None
        ordering_key = None

        for item in response:
            # Skip None values
            if item is None:
                continue

            # Check for explicit ResponseAttributes
            if isinstance(item, ResponseAttributes):
                if attributes is None:
                    attributes = dict(item.data)  # Access UserDict's data
                continue

            # Check for explicit ResponseOrderingKey
            if isinstance(item, ResponseOrderingKey):
                if ordering_key is None:
                    ordering_key = str(item.data)  # Access UserString's data
                continue

            # Everything else is the message body (take first non-None)
            if message_body is None:
                message_body = item

        # If we found a message body and at least one GCP type marker, create GCPResponse
        if message_body is not None and (
            attributes is not None or ordering_key is not None
        ):
            return GCPResponse(
                body=message_body,
                attributes=attributes,
                ordering_key=ordering_key,
            )

    # Fall back to base behavior
    return base_ensure_response(response)
