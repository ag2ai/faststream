from faststream import Logger
from faststream.asgi import AsgiFastStream, post, AsgiResponse, Request
from faststream.nats import NatsBroker

@post
async def log_request_payload(request: Request, logger: Logger) -> AsgiResponse:
    payload = await request.json()
    logger.info(payload)
    return AsgiResponse(status_code=200)


broker = NatsBroker()
app = AsgiFastStream(broker, asgi_routes=[
    ("/log-request-payload", log_request_payload),
])
