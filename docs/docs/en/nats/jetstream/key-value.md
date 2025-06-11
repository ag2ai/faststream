---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Key-Value Storage

## Overview

[*Key-Value*](https://docs.nats.io/nats-concepts/jetstream/key-value-store){.external-link target="_blank"} storage is just a high-level interface on top of *NatsJS*.

It is a regular *JetStream*, where the *KV key* is a subject.

`Put`/`Update` an object to *KV* by *key* - it's like publishing a new message to the corresponding subject in the stream.

Thus, the `Get` command returns not only the current *key* value but the latest one with an offset of it. Additionally, you can ask for a specific value based on its offset in the *KV* stream.

This interface provides you with rich abilities to use it like a regular *KV* storage (ignoring offset) + subscribe to *KV key* changes + ask for an old *KV value* revision. So you can use this feature in your application in a really different way. You can find some examples on the *NATS* developers' official [YouTube channel](https://youtube.com/@NATS_io?si=DWHvNFjsLruxg5OZ){.external-link target="_blank"}

## FastStream Details

**FastStream** has some useful methods to help you with **Key-Value NATS** feature interacting.

First of all, you need to create a *Key-Value* storage object and put some value to it:

```python linenums="1" hl_lines="9-10"
{! docs_src/nats/js/key_value.py [ln:1-5,12-16] !}
```

!!! tip
    `#!python broker.key_value(bucket="bucket")` is an idempotent method. It means that it stores all already created storages in memory and do not make new request to **NATS** if your are trying to call it for the same bucket.

---

Then we are able to use returned `key_value` object as a regular NATS one. But, if you want to watch by any changes by some key in the bucket, **FastStream** allows you to make it via regular `#!python @broker.subscriber` interface:

```python linenums="1" hl_lines="1"
{! docs_src/nats/js/key_value.py [ln:8-10] !}
```

Also, if you want more detail settings for you **Key Value Storage**, we have `KvWatch` object for it:

```python linenums="1" hl_lines="5"
from faststream.nats import NatsBroker, KvWatch

@broker.subscriber(
    "key",
    kv_watch=KvWatch("bucket", declare=False),
)
async def handler(msg: str):
    ...
```
