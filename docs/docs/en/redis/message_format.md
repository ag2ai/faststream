# Dealing with message encoded by FastStream

To provide such great features as observability and many others **FastStream** needs to add extra data to your message. **Redis** in turn provides ability to send any type of data inside the message. Since that, **FastStream** uses it's own binary format for messages that supports any type of data that you are going to use and add any additional information.


### Message structure

The message compiled by **FastStream** has the following structure:

```txt
# Format metadata
[Identification header: 8 bytes]
[Format version: 16 bit big-endian int]
[Number of headers: 16 bit big-endian int]
# headers
[Length of key: 16 bit big-endian int]
[Key: UTF-8 string]
[Length of value: 16 bit big-endian int]
[Value: UTF-8 string]
# and so on until headers length is reached ...

# The data
[Data: untouched user data until the end of the message]

```

!!! note
    The [...] blocks go one after another without any symbol or data between them

### Switching between formats

#### On Publisher's side

```python
from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.parser import BinaryMessageFormatV1

# JSONMessageFormat can be used instead, but it will be deprecated in future updates
broker = RedisBroker(message_format=BinaryMessageFormatV1)

app =  FastStream(broker)

@broker.publisher("queue")
async def message_publisher():
    return "message"
```

#### On Subscriber's side


```python
from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.parser import BinaryMessageFormatV1

broker = RedisBroker()

app =  FastStream(broker)

# JSONMessageFormat can be used instead, but it will be deprecated in future updates
@broker.subscriber("queue", message_format=BinaryMessageFormatV1)
async def message_handler(msg):
    print(msg)
```

### Parsing in FastStream application

Basically this message format is internal, but in some cases you will need to parse it correctly. For example in **on_receive** middleware's method:

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

You can always see the **FastStream's** implementation on [github](https://github.com/ag2ai/faststream).

### Parsing in Go

Go example parser:

```go
package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

var magicHeader = []byte{0x89, 0x42, 0x49, 0x4E, 0x0D, 0x0A, 0x1A, 0x0A}

type ParsedMessage struct {
	Version uint16
	Headers map[string]string
	Data    []byte
}

func ParseMessage(data []byte) (*ParsedMessage, error) {
	r := bytes.NewReader(data)

	// 1. Verify identity header
	identity := make([]byte, 8)
	if _, err := r.Read(identity); err != nil {
		return nil, err
	}
	if !bytes.Equal(identity, magicHeader) {
		return nil, errors.New("invalid magic header")
	}

	// 2. Read format version
	var version uint16
	if err := binary.Read(r, binary.BigEndian, &version); err != nil {
		return nil, err
	}

	// 3. Read number of headers
	var headerCount uint16
	if err := binary.Read(r, binary.BigEndian, &headerCount); err != nil {
		return nil, err
	}

	// 4. Parse headers
	headers := make(map[string]string)
	for i := 0; i < int(headerCount); i++ {
		// Key
		var keyLen uint16
		if err := binary.Read(r, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		keyBytes := make([]byte, keyLen)
		if _, err := r.Read(keyBytes); err != nil {
			return nil, err
		}

		// Value
		var valueLen uint16
		if err := binary.Read(r, binary.BigEndian, &valueLen); err != nil {
			return nil, err
		}

		valueBytes := make([]byte, valueLen)
		if _, err := r.Read(valueBytes); err != nil {
			return nil, err
		}

		headers[string(keyBytes)] = string(valueBytes)
	}

	// 5. Read remaining data
	messageData := make([]byte, r.Len())
	if _, err := r.Read(messageData); err != nil {
		return nil, err
	}

	return &ParsedMessage{
		Version: version,
		Headers: headers,
		Data:    messageData,
	}, nil
}

func main() {
	// Example usage
	message, err := GetMessage()
	if err != nil{
		panic(err)
	}
	parsed, err := ParseMessage(testMessage)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Version: %d\n", parsed.Version)
	fmt.Printf("Headers: %v\n", parsed.Headers)
	fmt.Printf("Data length: %d\n", len(parsed.Data))
	fmt.Printf("Data: %v\n", string(parsed.Data))
}

```

### Parsing in Java

Java example parser:

```java
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class BinaryFormatParser {

    // Expected magic header
    private static final byte[] MAGIC_HEADER = new byte[] {
        (byte) 0x89, 0x42, 0x49, 0x4E, 0x0D, 0x0A, 0x1A, 0x0A
    };

    public static class ParsedMessage {
        public int version;
        public Map<String, String> headers;
        public byte[] data;
    }

    public static ParsedMessage parse(byte[] message) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(message);

        // 1. Verify magic header
        byte[] magic = new byte[8];
        buffer.get(magic);
        if (!Arrays.equals(magic, MAGIC_HEADER)) {
            throw new IllegalArgumentException("Invalid magic header");
        }

        // 2. Read format version (unsigned short)
        int version = Short.toUnsignedInt(buffer.getShort());

        // 3. Read number of headers (unsigned short)
        int headerCount = Short.toUnsignedInt(buffer.getShort());

        // 4. Parse headers
        Map<String, String> headers = new HashMap<>();
        for (int i = 0; i < headerCount; i++) {
            // Key
            int keyLength = Short.toUnsignedInt(buffer.getShort());
            byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            String key = new String(keyBytes, StandardCharsets.UTF_8);

            // Value
            int valueLength = Short.toUnsignedInt(buffer.getShort());
            byte[] valueBytes = new byte[valueLength];
            buffer.get(valueBytes);
            String value = new String(valueBytes, StandardCharsets.UTF_8);

            headers.put(key, value);
        }

        // 5. Read remaining data
        byte[] data = new byte[buffer.remaining()];
        buffer.get(data);

        // Return parsed result
        ParsedMessage result = new ParsedMessage();
        result.version = version;
        result.headers = headers;
        result.data = data;
        return result;
    }

    public static void main(String[] args) throws Exception {
        // Example usage
        byte[] testMessage = getMessage();
        ParsedMessage parsed = parse(testMessage);

        System.out.println("Version: " + parsed.version);
        System.out.println("Headers: " + parsed.headers);
        System.out.println("Data length: " + parsed.data.length);
        System.out.println("Data: " + parsed.data);
    }
```
