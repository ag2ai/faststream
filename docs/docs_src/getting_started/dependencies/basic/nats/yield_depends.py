from faststream import FastStream, Depends
from faststream.nats import NatsBroker

broker = NatsBroker("nats://localhost:4222")
app = FastStream(broker)

class Session:
    closed = False

    def close(self) -> None:
        self.closed = True

async def get_session():
    session = Session()
    try:
        yield session
    finally:
        session.close()

@broker.subscriber("test")
async def handler(body: dict, session: Session = Depends(get_session)):
    assert not session.closed
