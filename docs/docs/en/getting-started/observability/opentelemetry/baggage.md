---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Baggage

[OpenTelemetry Baggage](https://opentelemetry.io/docs/concepts/signals/baggage/){.external-link target="_blank"} is a context propagation mechanism that allows you to pass custom metadata or key-value pairs across service boundaries, providing additional context for distributed tracing and observability.

## FastStream Baggage

**FastStream** provides a convenient abstraction over baggage that allows you to:

* **Initialize** the baggage
* **Propagate** baggage through headers
* **Modify** the baggage
* **Stop** propagating baggage

## Example

To initialize the baggage and start distributing it, follow this example:

```python linenums="1" hl_lines="3-4"
from faststream.opentelemetry import Baggage

headers = Baggage({"hello": "world"}).to_headers()
await broker.publish("hello", "first", headers=headers)
```

All interactions with baggage at the **consumption level** occurs through the **CurrentBaggage** object, which is automatically injected from the context:

```python linenums="1" hl_lines="6-10 17-18 24"
from faststream.opentelemetry import CurrentBaggage

@broker.subscriber("first")
@broker.publisher("second")
async def response_handler_first(msg: str, baggage: CurrentBaggage):
    print(baggage.get_all())  # {'hello': 'world'}
    baggage.remove("hello")
    baggage.set("user-id", 1)
    baggage.set("request-id", "UUID")
    print(baggage.get("user-id"))  # 1
    return msg


@broker.subscriber("second")
@broker.publisher("third")
async def response_handler_second(msg: str, baggage: CurrentBaggage):
    print(baggage.get_all())  # {'user-id': '1', 'request-id': 'UUID'}
    baggage.clear()
    return msg


@broker.subscriber("third")
async def response_handler_third(msg: str, baggage: CurrentBaggage):
    print(baggage.get_all())  # {}
```

!!! note
    If you consume messages in **batches**, then the baggage from each message will be merged into the **common baggage** available through the `get_all` method. However, you can still retrieve a list of all the baggage from the batch using the `get_all_batch` method.
