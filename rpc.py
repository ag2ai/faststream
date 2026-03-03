import logging

from faststream import FastStream
from faststream.rabbit import RabbitBroker
from pydantic import BaseModel, conint

broker = RabbitBroker()
app = FastStream(broker)


class In(BaseModel):
    i: str
    j: conint(gt=0)



class In2(BaseModel):
    i: str
    j: conint(gt=0)


class Out(BaseModel):
    k: str
    l: float


@broker.subscriber("RPC", description="This queue is used to for RPC")
@broker.publisher("RPC_OUT", description="This queue is used to for RPC")
async def rpc_handler(input: In) -> Out:
    logging.info(f"{input=}")
    return Out(k=input.i, l=input.j)