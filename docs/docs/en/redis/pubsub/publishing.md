---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Publishing

The **FastStream** `RedisBroker` supports all standard [publishing use cases](../../getting-started/publishing/index.md){.internal-link} similar to the `KafkaBroker`, allowing you to publish messages to Redis channels with ease.

Below you will find guidance on how to utilize the `RedisBroker` for publishing messages, including creating publisher objects and using decorators for streamlined publishing workflows.

## Basic Redis Channel Publishing

The `RedisBroker` allows you to publish messages directly to Redis channels. You can use Python primitives and `pydantic.BaseModel` to define the content of the message.

To publish a message to a Redis channel, follow these steps:

1. Create your RedisBroker instance

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/raw_publish.py [ln:14] !}
    ```

2. Publish a message using the `publish` method

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/raw_publish.py [ln:28.9] !}
    ```

This is the most straightforward way to use the RedisBroker to publish messages to Redis channels.

## Creating a publisher object

For a more structured approach and to include your publishers in the AsyncAPI documentation, it's recommended to create publisher objects. Here's how to do it:

1. Create your RedisBroker instance

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_object.py [ln:7] !}
    ```

2. Create a publisher instance for a specific channel

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_object.py [ln:16] !}
    ```

3. Publish a message using the `publish` method of the prepared publisher

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_object.py [ln:27.9] !}
    ```

When you encapsulate your broker within a FastStream object, the publisher will be documented in your service's AsyncAPI documentation.

## Decorating your publishing functions

Decorators in FastStream provide a convenient way to define the data flow within your application. The `RedisBroker` allows you to use decorators to publish messages to Redis channels, similar to the `KafkaBroker`.

By decorating a function with both `#!python @broker.subscriber(...)` and `#!python @broker.publisher(...)`, you create a DataPipeline unit that processes incoming messages and publishes the results to another channel. The order of decorators does not matter, but they must be applied to a function that has already been decorated by a `#!python @broker.subscriber(...)`.

The decorated function should have a return type annotation to ensure the correct interpretation of the return value before it's published.

Here's an example of using decorators with RedisBroker:

```python linenums="1"
{! docs_src/redis/pub_sub/publisher_decorator.py !}
```

1. **Initialize the RedisBroker instance:** Start by creating a RedisBroker instance.

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_decorator.py [ln:13] !}
    ```

2. **Prepare your publisher object to be used as a decorator:**

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_decorator.py [ln:17] !}
    ```

3. **Create your processing logic:** Implement a function that will process incoming messages and produce a response to be published to another Redis channel.

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_decorator.py [ln:22-23] !}
    ```

4. **Decorate your processing function:** Apply the `#!python @broker.subscriber(...)` and `#!python @broker.publisher(...)` decorators to your function to define the input channel and the output channel, respectively. Once your application is running, this decorated function will be triggered whenever a new message arrives on the `#!python "input_data"` channel, and it will publish the result to the `#!python "output_data"` channel.

    ```python linenums="1"
    {!> docs_src/redis/pub_sub/publisher_decorator.py [ln:20-23] !}
    ```
