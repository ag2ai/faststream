---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Channel Subscription

## Basic Channel Subscription

**Redis Pub/Sub** is the default subscriber type in **FastStream**, so you can simply create a regular `#!python @broker.subscriber("channel_name")` with a channel name and it creates a subscriber using **Redis Pub/Sub**.

In this example, we will build a FastStream application that listens to messages from the Redis channel named `#!python "test"`.

The complete application code is presented below:

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub.py!}
```

### Import FastStream and RedisBroker

To utilize the `#!python @broker.subscriber(...)` decorator for Redis channel subscription, you must first import FastStream and RedisBroker.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub.py [ln:1-2]!}
```

### Create a RedisBroker Instance

Create a `#!python RedisBroker` object and pass it to the `FastStream` object. This setup prepares the application for launch using the FastStream CLI.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub.py [ln:4-5]!}
```

### Define the Message Handler Function

Construct a function that will act as the consumer of messages from the `#!python "test"` channel and use the logger to output the message content.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub.py [ln:8-10]!}
```

When a message is published to the **Redis** channel `#!python "test"`, it will trigger the invocation of the decorated function. The message will be passed to the function's `msg` parameter, while the logger will be available for logging purposes.

## Pattern Channel Subscription

For subscribing to multiple Redis channels matching a pattern, use the `#!python @broker.subscriber(channel=PubSub("pattern", pattern=True))` decorator, where the channel argument receives a `PubSub` object with the pattern and pattern flag set to True.

Here's how to create a FastStream application that subscribes to all channels matching the `#!python "test.*"` pattern:

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub_pattern.py!}
```

### Use PubSub for Pattern Matching

Import the `PubSub` class from `faststream.redis` along with other necessary modules.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub_pattern.py [ln:1-2] !}
```

### Specify the Pattern for Channel Subscription

To define the pattern subscription, create a `PubSub` object with the desired pattern (`#!python "test.*"` in this case) and indicate that it's a pattern subscription by setting `pattern=True`.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub_pattern.py [ln:8] !}
```

### Create the Pattern Message Handler Function

Decide on a function that will act as the subscriber of messages from channels matching the specified pattern. Logging the messages is handled similarly as with basic channel subscription.

```python linenums="1"
{! docs_src/redis/pub_sub/channel_sub_pattern.py [ln:8-10] !}
```

With pattern channel subscription, when a message is published to a channel that matches the specified pattern (`#!python "test.*"`), our handler function will be invoked. The message is delivered to the `msg` argument of the function, similar to how it works in basic channel subscriptions.

### Pattern data access

You can also use the **Redis Pub/Sub** pattern feature to encode some data directly in the channel name. With **FastStream** you can easily access this data using the following code:

```python linenums="1" hl_lines="1 8 12"
{! docs_src/redis/pub_sub/pattern_data.py !}
```
