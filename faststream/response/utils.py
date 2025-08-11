from typing import Any

from .response import Response


def ensure_response(response: Response | Any) -> Response:
    if isinstance(response, Response):
        return response

    return Response(response)
