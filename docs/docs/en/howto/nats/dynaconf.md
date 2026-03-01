---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Dynamic Config with Nats

As **NATS** has key-value storage, you can use it as a configuration storage solution. Here is an example of how to use it with **FastStream**.

## Subscribing to Key-Value Changes

To subscribe to key-value changes, you can use the `KvWatch` class. The key to watch must be passed as a parameter to the class, which corresponds to a key in the storage.

When a message is received, the global context can be updated with the new value.

```python linenums="1" hl_lines="7-9"
from faststream import FastStream
from faststream.nats import NatsBroker, KvWatch

broker = NatsBroker()
app = FastStream(broker)

@broker.subscriber("create_sell", kv_watch=KvWatch("order_service"))
async def watch_kv_order_service(new_value: bool):
    app.context.set_global("create_sell", new_value)
```

## Checking the Parameter in the Subscriber

You can use `faststream.Context` to retrieve the parameter from the global context.

```python linenums="1" hl_lines="10"
from faststream import FastStream, Context
from faststream.nats import NatsBroker, NatsMessage

broker = NatsBroker()
app = FastStream(broker)

@broker.subscriber("order_service.order.filled.buy")
async def handle_filled_buy(
    message: NatsMessage,
    create_sell: bool = Context("create_sell"),
):
    if create_sell:
        await broker.publish(b"", "order_service.order.create.sell")
```

!!! note
    The KeyValue watcher will receive the latest value on startup.

??? example "Full Example"
    ```python linenums="1"
    from uuid import uuid4

    from faststream import FastStream, Context
    from faststream.nats import NatsMessage, NatsBroker, KvWatch

    broker = NatsBroker()
    app = FastStream(broker)

    @broker.subscriber("create_sell", kv_watch=KvWatch("order_service"))
    async def watch_kv_order_service(new_value: bool):
        app.context.set_global("create_sell", new_value)

    @broker.subscriber("order_service.order.filled.buy")
    async def handle_filled_buy(
        message: NatsMessage,
        create_sell: bool = Context("create_sell"),
    ):
        if create_sell:
            await broker.publish(b"", "order_service.order.create.sell")

    @broker.subscriber("order_service.order.create.sell")
    async def handle_create_sell(message: NatsMessage): ...

    @app.on_startup
    async def on_startup():
        await broker.connect()

        order_service_kv = await broker.key_value("order_service")

        initial_create_sell_value = b"1"
        await order_service_kv.put("create_sell", initial_create_sell_value)
        app.context.set_global("create_sell", bool(initial_create_sell_value))

    @app.after_startup
    async def after_startup():
        await broker.publish({"order_id": str(uuid4())}, "order_service.order.filled.buy")
    ```
