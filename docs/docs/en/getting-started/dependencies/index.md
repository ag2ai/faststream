---
search:
  boost: 10
---

# Dependencies

**FastStream** uses the secondary library [**FastDepends**](https://lancetnik.github.io/FastDepends/){.external-link target="_blank"} for dependency management.
This dependency system is literally borrowed from **FastAPI**, so if you know how to work with that framework, you'll be comfortable with dependencies in **FastStream**.

You can visit the [**FastDepends**](https://lancetnik.github.io/FastDepends/){.external-link target="_blank"} documentation for more details, but the key points and **additions** are covered here.

## Type Casting

The key function in the dependency management and type conversion system in **FastStream** is the decorator `#!python @apply_types` (also known as `#!python @inject` in **FastDepends**).

By default, it applies to all event handlers, unless you disabled the same option when creating the broker.

=== "AIOKafka"
    ```python
    from faststream.kafka import KafkaBroker
    broker = KafkaBroker(..., apply_types=False)
    ```

=== "Confluent"
    ```python
    from faststream.confluent import KafkaBroker
    broker = KafkaBroker(..., apply_types=False)
    ```

=== "RabbitMQ"
    ```python
    from faststream.rabbit import RabbitBroker
    broker = RabbitBroker(..., apply_types=False)
    ```

=== "NATS"
    ```python
    from faststream.nats import NatsBroker
    broker = NatsBroker(..., apply_types=False)
    ```

=== "Redis"
    ```python
    from faststream.redis import RedisBroker
    broker = RedisBroker(..., apply_types=False)
    ```

!!! warning
    Setting the `apply_types=False` flag not only disables type casting but also `Depends` and `Context`.
    If you want to disable only type casting, use `validate=False` instead.

This flag can be useful if you are using **FastStream** within another framework and you need to use its native dependency system.

## Dependency Injection

To implement dependencies in **FastStream**, a special class called **Depends** is used

=== "AIOKafka"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/dependencies/basic/kafka/depends.py !}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/dependencies/basic/confluent/depends.py !}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/dependencies/basic/rabbit/depends.py !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/dependencies/basic/nats/depends.py !}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="7-8"
    {!> docs_src/getting_started/dependencies/basic/redis/depends.py !}
    ```

**The first step**: You need to declare a dependency, which can be any `Callable` object.

??? note "Callable"
    A "Callable" is an object that can be "called". It can be a function, a class, or a class method.

    In other words, if you can write code like `my_object()` - `my_object` is `Callable`

=== "AIOKafka"
    ```python linenums="11" hl_lines="1"
    {!> docs_src/getting_started/dependencies/basic/kafka/depends.py [ln:11-12] !}
    ```

=== "Confluent"
    ```python linenums="11" hl_lines="1"
    {!> docs_src/getting_started/dependencies/basic/confluent/depends.py [ln:11-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="11" hl_lines="1"
    {!> docs_src/getting_started/dependencies/basic/rabbit/depends.py [ln:11-12] !}
    ```

=== "NATS"
    ```python linenums="11" hl_lines="1"
    {!> docs_src/getting_started/dependencies/basic/nats/depends.py [ln:11-12] !}
    ```

=== "Redis"
    ```python linenums="11" hl_lines="1"
    {!> docs_src/getting_started/dependencies/basic/redis/depends.py [ln:11-12] !}
    ```

**Second step**: Declare which dependencies you need using `Depends`

=== "AIOKafka"
    ```python linenums="11" hl_lines="2"
    {!> docs_src/getting_started/dependencies/basic/kafka/depends.py [ln:11-12] !}
    ```

=== "Confluent"
    ```python linenums="11" hl_lines="2"
    {!> docs_src/getting_started/dependencies/basic/confluent/depends.py [ln:11-12] !}
    ```

=== "RabbitMQ"
    ```python linenums="11" hl_lines="2"
    {!> docs_src/getting_started/dependencies/basic/rabbit/depends.py [ln:11-12] !}
    ```

=== "NATS"
    ```python linenums="11" hl_lines="2"
    {!> docs_src/getting_started/dependencies/basic/nats/depends.py [ln:11-12] !}
    ```

=== "Redis"
    ```python linenums="11" hl_lines="2"
    {!> docs_src/getting_started/dependencies/basic/redis/depends.py [ln:11-12] !}
    ```

**The last step**: Just use the result of executing your dependency!

It's easy, isn't it?

!!! tip "Auto `#!python @apply_types`"
    In the code above, we didn't use this decorator for our dependencies. However, it still applies
    to all functions used as dependencies. Please keep this in your mind.

## Top-level Dependencies

If you don't need a dependency result, you can use the following code:

```python
@broker.subscriber("test")
def method(_ = Depends(...)): ...
```

But, using a special `subscriber` parameter is much more suitable:

```python
@broker.subscriber("test", dependencies=[Depends(...)])
def method(): ...
```

You can also declare broker-level dependencies, which will be applied to all broker's handlers:

```python
broker = RabbitBroker(dependencies=[Depends(...)])
```

## Nested Dependencies

Dependencies can also contain other dependencies. This works in a very predictable way: just declare
`Depends` in the dependent function.

=== "AIOKafka"
    ```python linenums="1" hl_lines="7-8 10-11 16-17"
    {!> docs_src/getting_started/dependencies/basic/kafka/nested_depends.py !}
    ```

    1. A nested dependency is called here

=== "Confluent"
    ```python linenums="1" hl_lines="7-8 10-11 16-17"
    {!> docs_src/getting_started/dependencies/basic/confluent/nested_depends.py !}
    ```

    1. A nested dependency is called here

=== "RabbitMQ"
    ```python linenums="1" hl_lines="7-8 10-11 16-17"
    {!> docs_src/getting_started/dependencies/basic/rabbit/nested_depends.py !}
    ```

    1. A nested dependency is called here

=== "NATS"
    ```python linenums="1" hl_lines="7-8 10-11 16-17"
    {!> docs_src/getting_started/dependencies/basic/nats/nested_depends.py !}
    ```

    1. A nested dependency is called here

=== "Redis"
    ```python linenums="1" hl_lines="7-8 10-11 16-17"
    {!> docs_src/getting_started/dependencies/basic/redis/nested_depends.py !}
    ```

    1. A nested dependency is called here

!!! Tip "Caching"
    In the example above, the `another_dependency` function will be called at **ONCE**!
    **FastDepends** caches all dependency execution results within **ONE** `#!python @apply_types` call stack.
    This means that all nested dependencies will receive the cached result of dependency execution.
    But, between different calls of the main function, these results will be different.

    To prevent this behavior, just use `#!python Depends(..., cache=False)`. In this case, the dependency will be used for each function
    in the call stack where it is used.

## Use with Regular Functions

You can use the decorator `#!python @apply_types` not only with `#!python @broker.subscriber(...)`, but also with regular functions, both synchronous and asynchronous.

=== "Sync"
    ```python hl_lines="3-4" linenums="1"
    {!> docs_src/getting_started/dependencies/basic/sync.py !}
    ```

=== "Async"
    ```python hl_lines="5-6 8-9" linenums="1"
    {!> docs_src/getting_started/dependencies/basic/async_.py !}
    ```

    !!! tip "Be careful"
        In asynchronous code, you can use both synchronous and asynchronous dependencies.
        But in synchronous code, only synchronous dependencies are available to you.

## Casting Dependency Types

**FastDepends**, used by **FastStream**, also gives the type `return`. This means that the value returned by the dependency will be
be cast to the type twice: as `return` for dependencies and as the input argument of the main function. This does not incur additional costs if
these types have the same annotation. Just keep it in mind. Or not... Anyway, I've warned you.

```python linenums="1"
from faststream import Depends, apply_types

def simple_dependency(a: int, b: int = 3) -> str:
    return a + b  # 'return' is cast to `str` for the first time

@apply_types
def method(a: int, d: int = Depends(simple_dependency)):
    # 'd' is cast to `int` for the second time
    return a + d

assert method("1") == 5
```

Also, the result of executing the dependency is cached. If you use this dependency in `N` functions,
this cached result will be converted to type `N` times (at the input to the function being used).

To avoid problems with this, use [mypy](https://www.mypy-lang.org){.external-link target="_blank"} or just be careful with the annotation
of types in your project.
