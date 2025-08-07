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

You can provide a raw message along with any broker-specific options if needed:

```shell
faststream publish main:app '{"name": "John"}' --channel 'my-topic'
```
