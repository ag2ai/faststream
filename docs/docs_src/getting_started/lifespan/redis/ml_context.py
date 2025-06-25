from contextlib import asynccontextmanager

from faststream import Context, ContextRepo, FastStream
from faststream.redis import RedisBroker

broker = RedisBroker("redis://localhost:6379")


def fake_ml_model_answer(x: float) -> float:
    return x * 42


@asynccontextmanager
async def lifespan(context: ContextRepo):
    # load fake ML model
    ml_models = {"answer_to_everything": fake_ml_model_answer}
    context.set_global("model", ml_models)

    yield

    # Clean up the ML models and release the resources
    ml_models.clear()


@broker.subscriber("test")
async def predict(x: float, model: dict = Context()):
    result = model["answer_to_everything"](x)
    return {"result": result}


app = FastStream(broker, lifespan=lifespan)
