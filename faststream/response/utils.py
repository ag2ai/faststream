from typing import Any, Union

from .response import Response


def ensure_response(response: Union[Response, Any]) -> Response:
    if isinstance(response, Response):
        return response

    return Response(response)
