from faststream.asgi import AsgiFastStream, get, AsgiResponse, Header, Query
from faststream.nats import NatsBroker


@get
async def protected_method(
    token: str = Header("X-auth-token"),
    foo: list[str] = Query(),
):
    if token != "secret-token":
        return AsgiResponse(status_code=401)
    print(foo)
    return AsgiResponse(status_code=200)


broker = NatsBroker()
app = AsgiFastStream(broker, asgi_routes=[
    ("/protected-method", protected_method),
])
