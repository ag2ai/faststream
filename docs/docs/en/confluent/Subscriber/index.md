---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Basic Subscriber

To start consuming from a **Kafka** topic, simply decorate your consuming function with a `#!python @broker.subscriber(...)` decorator, passing a string as a topic key.

In the following example, we will create a simple FastStream app that will consume `HelloWorld` messages from a `#!python "hello_world"` topic.

The full app code looks like this:

```python linenums="1"
{! docs_src/confluent/consumes_basics/app.py!}
```

## Import FastStream and KafkaBroker

To use the `#!python @broker.subscriber(...)` decorator, first, we need to import the base FastStream app KafkaBroker to create our broker.

```python linenums="1"
{! docs_src/confluent/consumes_basics/app.py [ln:3-4] !}
```

## Define the HelloWorld Message Structure

Next, you need to define the structure of the messages you want to consume from the topic using Pydantic. For the guide, we’ll stick to something basic, but you are free to define any complex message structure you wish in your project.

```python linenums="1"
{! docs_src/confluent/consumes_basics/app.py [ln:7-12] !}
```

## Create a KafkaBroker

Next, we will create a `KafkaBroker` object and wrap it into the `FastStream` object so that we can start our app using CLI later.

```python linenums="1"
{! docs_src/confluent/consumes_basics/app.py [ln:15-16] !}
```

## Create a Function that will Consume Messages from a Kafka hello-world Topic

Let’s create a consumer function that will consume `HelloWorld` messages from `#!python "hello_world"` topic and log them.

```python linenums="1"
{! docs_src/confluent/consumes_basics/app.py [ln:19-21] !}
```

The function decorated with the `#!python @broker.subscriber(...)` decorator will be called when a message is produced to **Kafka**.

The message will then be injected into the typed `msg` argument of the function, and its type will be used to parse the message.

In this example case, when the message is sent to a `#!python "hello_world"` topic, it will be parsed into a `HelloWorld` class, and the `on_hello_world` function will be called with the parsed class as the `msg` argument value.
