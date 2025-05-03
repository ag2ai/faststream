from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union, overload

from faststream.asgi.response import AsgiResponse

if TYPE_CHECKING:
    from faststream.asgi.types import ASGIApp, Receive, Scope, Send, UserApp

class GetHandler:
    def __init__(self, func: "UserApp", include_in_schema: bool = True, description: Optional[str] = None):
        self.func = func
        self.include_in_schema = include_in_schema
        self.methods = ("GET", "HEAD")
        self.description = description or func.__doc__

    async def __call__(self, scope: "Scope", receive: "Receive", send: "Send") -> None:
        if scope["method"] not in self.methods:
            response: ASGIApp =  _get_method_not_allowed_response(self.methods)

        else:
            try:
                response = await self.func(scope)
            except Exception:
                response = AsgiResponse(body=b"Internal Server Error", status_code=500)

        await response(scope, receive, send)
        return

@overload
def get(func: "UserApp", *, include_in_schema: bool = True, description: Optional[str] = None) -> "ASGIApp": ...


@overload
def get(
    func: None = None,
    *,
    include_in_schema: bool = True,
    description: Optional[str] = None
) -> Callable[["UserApp"], "ASGIApp"]: ...


def get(
    func: Optional["UserApp"] = None, *, include_in_schema: bool = True, description: Optional[str] = None
) -> Union[Callable[["UserApp"], "ASGIApp"], "ASGIApp"]:

    if func is None:

        def decorator(inner_func: "UserApp") -> "ASGIApp":
            return GetHandler(inner_func, include_in_schema=include_in_schema, description=description)  # type: ignore
        return decorator

    return GetHandler(func, include_in_schema=include_in_schema, description=description)


def _get_method_not_allowed_response(methods: Sequence[str]) -> AsgiResponse:
    return AsgiResponse(
        body=b"Method Not Allowed",
        status_code=405,
        headers={
            "Allow": ", ".join(methods),
        },
    )
