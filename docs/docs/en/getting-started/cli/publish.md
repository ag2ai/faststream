## Publishing messages

**FastStream CLI** lets you publish messages directly to a broker.

```shell
faststream publish --help
```

```{ .console .no-copy }
Usage: faststream publish [OPTIONS] APP MESSAGE

Publish a message using the specified broker in a FastStream application.
This command publishes a message to a broker configured in a FastStream app instance. It supports various brokers and can handle extra arguments specific to each broker type. These are parsed and passed to the broker's publish method.

Options:
  --app-dir TEXT Look for APP in the specified directory, by adding this to the PYTHONPATH. [env var: FASTSTREAM_APP_DIR] [default: .]
  --rpc --no-rpc Enable RPC mode and system output. [default: no-rpc]
  --factory -f Treat APP as an application factory.
  --help Show this message and exit.
```
{ data-search-exclude }

You can provide a raw message along with any broker-specific options if needed.

To publish message and process it in a subscriber, create **FastStream** app in the file `main.py`:

```python
from faststream import FastStream as FastStreamApp
from faststream.nats import NatsBroker

broker = NatsBroker()
app = FastStreamApp(broker).as_asgi()

@broker.subscriber(subject="my-subject")
async def new_user(msg: dict[str, str]):
    print(f"Created new user: {msg}")
```

Run the **Nats** broker using docker:

```shell
docker run --rm -p 4222:4222 nats
```

Run **FastStream** app:

```shell
faststream run app:app
```

And finally, publish message to subject `my-subject`:

```shell
faststream publish main:app '{"name": "John"}' --subject 'my-subject'
```

See following output in terminal where app is running:
```{ .console .no-copy }
2025-08-13 17:58:54,032 INFO     - my-subject | 7d9f1727-a - Received
Created new user: {'name': 'John'}
2025-08-13 17:58:54,032 INFO     - my-subject | 7d9f1727-a - Processed
```
