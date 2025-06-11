---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Object Storage

Object storage is almost identical to the [*Key-Value*](./key-value.md) storage concept, so you can reuse the guide.

## Overview

[*Object Storage*](https://docs.nats.io/nats-concepts/jetstream/obj_store){.external-link target="_blank"} is just a high-level interface on top of *NatsJS*.

It is a regular *JetStream*, where the *Object key* is a subject.

The main difference between *KV* and *Object* storages is that in the *Object* storage, you can store files greater than **1MB** (a limitation of *KV*). It has no limit on the maximum object size and stores it in chunks (each message is an object chunk), so you can literally *stream* huge objects through *NATS*.

## FastStream Details

**FastStream** has some useful methods to help you with **Object Storage NATS** feature interacting.

First of all, you need to create a *Object Storage* object and put some value to it:

```python linenums="1" hl_lines="11-12"
{! docs_src/nats/js/object.py [ln:1-2,3,5,7-10,23-26] !}
```

!!! tip
    * [`BytesIO`](https://docs.python.org/3/library/io.html#binary-i-o){.external-link target="_blank"} - is a *Readable* object used to emulate a file opened for reading.

    * `#!python broker.object_storage(bucket="example-bucket")` is an idempotent method. It means that it stores all already created storages in memory and do not make new request to **NATS** if your are trying to call it for the same bucket.

---

Then we are able to use returned `object_storage` object as a regular NATS one. But, if you want to watch by any new files in the bucket, **FastStream** allows you to make it via regular `#!python @broker.subscriber` interface:

```python linenums="1" hl_lines="1"
@broker.subscriber("example-bucket", obj_watch=True)
async def handler(filename: str):
    assert filename == "file.txt"
```

**NATS** delivers you just a filename (and some more metainformation you can get access via `message.raw_message`) because files can be any size. The framework should protect your service from memory overflow, so we can't upload whole file content right to the memo. By you can make it manually the following way:

```python linenums="1" hl_lines="1 6 10-11"
{! docs_src/nats/js/object.py [ln:6-7,12-20] !}
```

!!! note
    `faststream.nats.annotations.ObjectStorage` is a your current bucket, so you need no to put it to context manually.

Also, if you want more detail settings for you **Object Storage**, we have `ObjWatch` object for it:

```python linenums="1" hl_lines="5"
from faststream.nats import NatsBroker, ObjWatch

@broker.subscriber(
    "example-bucket",
    obj_watch=ObjWatch(declare=False),
)
async def handler(filename: str):
    ...
```
