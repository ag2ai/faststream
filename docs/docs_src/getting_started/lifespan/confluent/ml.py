from faststream import Context, ContextRepo, FastStream
from faststream.confluent import KafkaBroker

broker = KafkaBroker("localhost:9092")
app = FastStream(broker)

ml_models = {}  # fake ML model


def fake_answer_to_everything_ml_model(x: float) -> float:
    return x * 42


@app.on_startup
async def setup_model(context: ContextRepo):
    # Load the ML model
    ml_models["answer_to_everything"] = fake_answer_to_everything_ml_model
    context.set_global("model", ml_models)


@app.on_shutdown
async def shutdown_model(model: dict = Context()):
    # Clean up the ML models and release the resources
    model.clear()


@broker.subscriber("test")
async def predict(x: float, model: dict = Context()):
    result = model["answer_to_everything"](x)
    return {"result": result}
