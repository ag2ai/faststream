# Dealing with message encoded by FastStream

To provide great features like observability and more, **FastStream** needs to include extra data in your message. **Redis**, in turn, provides the ability to send any type of data within a message. Therefore, **FastStream** uses its own binary format for messages, which supports any type of data you want to use and can include any additional information.

### Migration history

The binary format replaced JSON as the default over several releases:

- In versions 0.5.x - JSON was the default message format
- In versions 0.6.x - the binary format became the default, with JSON deprecated
- In versions 0.7.x and beyond - the dedicated JSON format (`JSONMessageFormat`) is removed; the binary format is the only built-in one

If you communicate with external, non-**FastStream** systems that still send plain JSON, you don't need the old format — the binary parser falls back to raw JSON automatically. See [Interoperability with non-FastStream systems](#interoperability-with-non-faststream-systems).

### Message structure

The message compiled by **FastStream** in binary format has the following structure:

```txt
# Format metadata
[Identification header: 8 bytes]
[Format version: 16 bit big-endian int]
[Headers offset (points to the number of headers): 32 bit big-endian uint]
[Data offset (indicates position at which the data starts): 32 bit big-endian uint]
[Number of headers: 16 bit big-endian uint]
# headers
[Length of key: 16 bit big-endian uint]
[Key: UTF-8 string]
[Length of value: 16 bit big-endian uint]
[Value: UTF-8 string]
# and so on until headers length is reached ...

# The data
[Data: untouched user data until the end of the message]

```

!!! note
    The [...] blocks are arranged in a sequence, with no symbols or data separating them.
### Switching between formats

`BinaryMessageFormatV1` is the only built-in format and the default one, so you usually don't need to set it explicitly. The `message_format` option exists so you can plug in your own [custom format](#custom-message-format) implementation.

!!! warning
    The old `JSONMessageFormat` was deprecated in **0.6.x** and **removed in 0.7.0**. If you upgraded from `0.5.x`/`0.6.x` and relied on it for compatibility with external systems, see [Interoperability with non-FastStream systems](#interoperability-with-non-faststream-systems) below — you no longer need a dedicated JSON format, because `BinaryMessageFormatV1` already falls back to plain JSON automatically.

#### On the publisher's side

```python
from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.parser import BinaryMessageFormatV1

broker = RedisBroker(message_format=BinaryMessageFormatV1)

app =  FastStream(broker)

@broker.publisher("queue")
async def message_publisher():
    return "message"
```

#### On the subscriber's side


```python
from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.parser import BinaryMessageFormatV1

broker = RedisBroker()

app =  FastStream(broker)

@broker.subscriber("queue", message_format=BinaryMessageFormatV1)
async def message_handler(msg):
    print(msg)
```

### Interoperability with non-FastStream systems

A common need is to exchange messages with applications that are **not** built with **FastStream** (services written in other languages, plain `redis-py` producers, etc.). Such systems don't know about the binary format described above, so they publish **raw payloads** — usually plain JSON.

You don't need a special message format for this. `BinaryMessageFormatV1.parse` automatically **falls back** to non-binary payloads, so a **FastStream** subscriber consumes raw messages out of the box. When the incoming bytes don't start with the binary identity header, the parser tries, in order:

1. A **legacy JSON envelope** — `{"data": ..., "headers": {...}}` — produced by old (`<= 0.5.x`) **FastStream** versions or by systems that mimic that shape. Both `data` and `headers` are extracted.
2. **Any other bytes** — treated as a raw payload with empty headers. The body is still decoded normally afterwards (JSON is parsed into a Python object, plain text is kept as a string), so a message like `b'{"id": "1", "name": "John"}'` arrives in your handler as a `dict`.

#### Consuming raw JSON from an external producer

A non-**FastStream** producer can publish plain JSON bytes directly:

```python
# Any external client, e.g. plain redis-py
await redis.publish("test", b'{"id": "1", "name": "John"}')
```

And a regular **FastStream** subscriber receives the decoded payload without any extra configuration:

```python
from faststream import FastStream
from faststream.redis import RedisBroker

broker = RedisBroker()
app = FastStream(broker)

@broker.subscriber("test")
async def handler(msg: dict[str, Any]) -> None:
    # {"id": "1", "name": "John"}
    print(msg)
```

#### Publishing to an external consumer

When you publish with **FastStream** (`broker.publish(...)` or a `@broker.publisher`), the message is wrapped in the binary format. A non-**FastStream** consumer can still read it, but it has to parse that format — see the [Go](#parsing-in-go) and [Java](#parsing-in-java) examples below, which decode the envelope into data + headers.

If the other side cannot (or should not) parse the binary format and you only need to send a raw payload, publish bytes through the underlying **Redis** client directly. `broker._connection` is the native `redis.asyncio.Redis` instance (also available inside handlers via the `Redis` annotation from `faststream.redis.annotations`, see [Context](../getting-started/context.md)):

```python
import json

from faststream.redis import Redis

@broker.subscriber("test")
async def handler(msg, redis: Redis) -> None:
    # Pub/Sub channel
    await redis.publish(
        "test",
        json.dumps({"id": "1", "name": "John"}).encode(),
    )
```

This bypasses the **FastStream** envelope entirely, so the external consumer receives exactly the bytes you sent. Because you are calling the native client, use the method that matches the destination type — `publish` for channels, `rpush` for [lists](./list/index.md), `xadd` for [streams](./streams/index.md):


### Custom message format

If neither the binary format nor the JSON fallback fits your needs, implement your own format by subclassing `MessageFormat` and providing `encode`/`parse`:

```python
from typing import Any
from faststream.redis.parser import MessageFormat


class MyMessageFormat(MessageFormat):
    @classmethod
    async def encode(cls, *, message, reply_to, headers, correlation_id, serializer=None, codec=None) -> bytes:
        ...

    @classmethod
    def parse(cls, data: bytes) -> tuple[bytes, dict[str, Any]]:
        ...
```

Then pass it via `message_format=` on the broker, publisher, or subscriber exactly like `BinaryMessageFormatV1`.

### Parsing in FastStream application

This message format is primarily internal, but there are some cases where you may need to correctly parse it. For example, in the **on_receive** method of the middleware:

```python
from faststream import BaseMiddleware
from faststream.redis.parser import BinaryMessageFormatV1


class MyMiddleware(BaseMiddleware):
    async def on_receive(self) -> None:
        data, headers = BinaryMessageFormatV1.parse(self.msg["data"])
        data *= 2
        self.msg["data"] = BinaryMessageFormatV1.encode(
            message=data,
            reply_to=None,
            correlation_id=headers["correlation_id"],
            headers=headers,
        )
```

### Parsing without FastStream

You can always find the **FastStream's** implementation on [github](https://github.com/ag2ai/faststream).

### Parsing in Go

Go example parser:

```go
package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
)

func main() {
	// Read entire file
	raw, err := GetMessage()
	if err != nil {
		fmt.Println("Error reading file:", err)
		os.Exit(1)
	}

	// Parse fixed metadata
	idHeader := raw[0:8]
	version := binary.BigEndian.Uint16(raw[8:10])
	headersOffset := binary.BigEndian.Uint32(raw[10:14])
	dataOffset := binary.BigEndian.Uint32(raw[14:18])

	// Parse headers section
	buf := bytes.NewBuffer(raw[headersOffset:])
	headerCount := int(binary.BigEndian.Uint16(buf.Next(2)))

	headers := make(map[string]string)
	for i := 0; i < headerCount; i++ {
		keyLen := int(binary.BigEndian.Uint16(buf.Next(2)))
		key := string(buf.Next(keyLen))
		valLen := int(binary.BigEndian.Uint16(buf.Next(2)))
		value := string(buf.Next(valLen))
		headers[key] = value
	}

	// Parse data section (assumed to be UTF-8 text)
	data := string(raw[dataOffset:])

	// Print results
	fmt.Printf("ID Header: % x\n", idHeader)
	fmt.Printf("Version: %d\n", version)
	fmt.Printf("Headers: %v\n", headers)
	fmt.Printf("Data: %s\n", data)
}

```

### Parsing in Java

Java example parser:

```java
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

class MessageParser {

    public static void main(String[] args) throws Exception {
        // Read entire file into byte array
        byte[] fileData = getMessage();

        // Parse using fixed offsets
        ByteBuffer buffer = ByteBuffer.wrap(fileData).order(ByteOrder.BIG_ENDIAN);

        // Parse header
        byte[] idHeader = new byte[8];
        buffer.get(idHeader);
        int version = buffer.getShort() & 0xFFFF;
        int headersOffset = buffer.getInt();
        int dataOffset = buffer.getInt();

        // Jump to headers section
        buffer.position(headersOffset);
        int headerCount = buffer.getShort() & 0xFFFF;

        // Parse key-value pairs
        Map<String, String> headers = new LinkedHashMap<>();
        for (int i = 0; i < headerCount; i++) {
            String key = readString(buffer);
            String value = readString(buffer);
            headers.put(key, value);
        }

        // Extract and convert text data
        byte[] dataBytes = new byte[fileData.length - dataOffset];
        System.arraycopy(fileData, dataOffset, dataBytes, 0, dataBytes.length);
        String data = new String(dataBytes, StandardCharsets.UTF_8);

        // Print results
        System.out.println("ID Header: " + bytesToHex(idHeader));
        System.out.println("Version: " + version);
        System.out.println("Headers: " + headers);
        System.out.println("Data: " + data);
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getShort() & 0xFFFF;
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder hex = new StringBuilder();
        for (byte b : bytes) {
            hex.append(String.format("%02X ", b));
        }
        return hex.toString().trim();
    }
}
```
