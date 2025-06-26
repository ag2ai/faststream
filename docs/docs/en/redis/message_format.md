# Dealing with message encoded by FastStream

To provide such great features as observability and many others **FastStream** needs to add extra data to your message. **Redis** in turn provides ability to send any type of data inside the message. Since that, **FastStream** uses it's own binary format for messages that supports any type of data that you are going to use and add any additional information.


### Migration plan

Currently **FastStream** uses JSON by default to encode and parse messages internally. But in future updates it will be deprecated and removed according to the following plan:

- in versions 0.5.* - JSON will stay as default message format
- in versions 0.6.* - JSON will be deprecated and new binary format will be used as default
- in versions 0.7.* - JSON message format will be completely removed


### Message structure

The message compiled by **FastStream** has the following structure:

```txt
# Format metadata
[Identification header: 8 bytes]
[Format version: 16 bit big-endian int]
[Headers offset (points to the number of headers): 32 bit big-endian int]
[Data offset (indicates position at which the data starts): 32 bit big-endian int]
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
