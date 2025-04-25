---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Using FastStream with Django

[**Django**](https://www.djangoproject.com/){.external-link target="_blank"} is a high-level Python web framework that encourages rapid development and clean, pragmatic design. Built by experienced developers, it takes care of much of the hassle of web development, so you can focus on writing your app without needing to reinvent the wheel. It’s free and open source.

In this tutorial, let's see how to use the FastStream app alongside a **Django** app.

## ASGI

[**ASGI**](https://asgi.readthedocs.io/en/latest/){.external-link target="_blank"} protocol supports lifespan events, and **Django** can be served as an **ASGI** application. So, the best way to integrate FastStream with the **Django** is by using **ASGI** lifespan. You can write it by yourself (it is really easy) or use something like [this](https://github.com/illagrenan/django-asgi-lifespan){.external-link target="_blank"}, but the preferred way for us is using [**Starlette**](https://www.starlette.io/){.external-link target="_blank"} Router.

Starlette Router allows you to serve any **ASGI** application you want, and it also supports lifespans. So, you can use it in your project to serve your regular Django **ASGI** and start up your FastStream broker too. Additionally, Starlette has much better static files support, providing an extra zero-cost feature.

## Default Django Application

Well, lets take a look at a default **Django** `asgi.py`

```python linenums="1" title="asgi.py"
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")

application = get_asgi_application()
```

You can already serve it using any **ASGI** server

For example, using [**uvicorn**](https://www.uvicorn.org/deployment/){.external-link target="_blank"}:

```bash
uvicorn asgi:app --workers 4
```

Or you can use [**Gunicorn**](https://docs.gunicorn.org/en/latest/run.html){.external-link target="_blank"} with uvicorn workers

```bash
gunicorn asgi:app --workers 4 --worker-class uvicorn.workers.UvicornWorker
```

Your **Django** views, models and other stuff has no any changes if you serving it through **ASGI**, so you need no worry about it.

## FastStream Integration

### Serving Django via Starlette

Now, we need to modify our `asgi.py` to serve it using **Starlette**

```python linenums="1" title="asgi.py" hl_lines="16"
# regular Djano stuff
import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")

django_asgi = get_asgi_application()

# Starlette serving
from starlette.applications import Starlette
from starlette.routing import Mount

application = Starlette(
    routes=(
        Mount("/", django_asgi()),  # redirect all requests to Django
    ),
)
```

### Serving static files with Starlette

Also, **Starlette** has a better static files provider than original **Django** one, so we can reuse it too.

Just add this line to your `settings.py`

```python title="settings.py"
STATIC_ROOT = "static/"
```

And collect all static files by default **Django** command

```bash
python manage.py collectstatic
```

It creates a `static/` directory in the root of your project, so you can serve it using **Starlette**

```python linenums="1" title="asgi.py" hl_lines="8"
# Code above omitted 👆

from starlette.staticfiles import StaticFiles

application = Starlette(
    routes=(
        # /static is your STATIC_URL setting
        Mount("/static", StaticFiles(directory="static"), name="static"),
        Mount("/", get_asgi_application()),  # regular Django ASGI
    ),
)
```

### FastStream lifespan

Finally, we can add our **FastStream** integration like a regular lifespan

```python linenums="1" title="asgi.py" hl_lines="18"
# Code above omitted 👆

from contextlib import asynccontextmanager
from faststream.kafka import KafkaBroker

broker = KafkaBroker()

@asynccontextmanager
async def broker_lifespan(app):
    await broker.start()
    try:
        yield
    finally:
        await broker.close()

application = Starlette(
    ...,
    lifespan=broker_lifespan,
)
```

!!! note
    The code imports `KafkaBroker` as our application is going to connect with **Kafka**. Depending on your requirements, import the necessary service's broker from the options provided by **FastStream**, such as `RabbitBroker`, `NatsBroker` or `KafkaBroker`.

??? example "Full Example"
    ```python linenums="1" title="asgi.py"
    import os
    from contextlib import asynccontextmanager

    from django.core.asgi import get_asgi_application
    from starlette.applications import Starlette
    from starlette.routing import Mount
    from starlette.staticfiles import StaticFiles
    from faststream.kafka import KafkaBroker


    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")

    broker = KafkaBroker()

    @asynccontextmanager
    async def broker_lifespan(app):
        await broker.start()
        try:
            yield
        finally:
            await broker.close()

    application = Starlette(
        routes=(
            Mount("/static", StaticFiles(directory="static"), name="static"),
            Mount("/", get_asgi_application()),
        ),
        lifespan=broker_lifespan,
    )
    ```

This way we can easily integrate our **FastStream** application with the **Django**!

# Accessing Django ORM from within a FastStream Consumer
## Start using FastStream CLI

In order to access the **Django** ORM from within a FastStream consumer, you need to ensure that the Django settings are properly configured and that the Django application is initialized before accessing the ORM. Here is how to do it using `serve_faststream.py` as an entry point for your FastStream consumer application.

```
project-root/
├── manage.py
├── serve_faststream.py
```
```Python
# serve_faststream.py
# ruff: noqa: E402
###############################################################################
import os

import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
django.setup()
# These lines are necessary to set up Django, call them before any Django Related imports.
###############################################################################
from django.contrib.auth.models import User  # This line must be after django.setup()

from faststream import FastStream
from faststream.rabbit import RabbitBroker


broker = RabbitBroker("amqp://guest:guest@localhost:5672")

@broker.subscriber("demo")
async def faststream_django_orm_demo_handler(message: str):
    """
    This demonstrates how to access Django ORM from within a FastStream consumer.
    """
    qs = User.objects.all()
    async for user in qs:  # async django ORM is accessible
        print(user)
    print(message)

app = FastStream(broker)
```

Start consumer with:
```bash
faststream run serve_faststream:app
```

It is advisable to use FastStream's router to keep `serve_faststream.py` clean and to ensure `django.setup()` is always called first. See [FastStream Router](https://faststream.airt.ai/latest/getting-started/routers/) for more information.
## Start using Django's management command
This demonstrates how to access Django ORM from within a FastStream consumer by using Django's management command. When managenment command runs, it automatically sets up Django and makes the ORM available.
```Python
# serve_faststream.py
import sys

# From previous example, this run django.setup() only when started by FastStream CLI
# Remove this block entirely if you are not using FastStream CLI
if "bin/faststream" in sys.argv[0]:
    import os

    import django

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "app.settings")
    django.setup()

from django.contrib.auth.models import User

from faststream import FastStream
from faststream.rabbit import RabbitBroker


broker = RabbitBroker("amqp://guest:guest@localhost:5672")

@broker.subscriber("demo")
async def faststream_django_orm_demo_handler(message: str):
    """
    This demonstrates how to access Django ORM from within a FastStream consumer.
    """
    qs = User.objects.all()
    async for user in qs:  # async django ORM is accessible
        print(user)
    print(message)

app = FastStream(broker)
```
Create a management command in your Django app:
```
project-root/
├──<your-django-app-name>/
    ├──management/
        ├──commands/
            ├── __init__.py
            ├── faststream.py
├── manage.py
├── serve_faststream.py
```
```Python
# faststream.py
import asyncio

from django.core.management.base import BaseCommand
from serve_faststream import app as faststream_app


class Command(BaseCommand):
    help = "Start FastStream consumer"

    def handle(self, *args, **options):
        asyncio.run(faststream_app.run())
```
You can now start consumer with:
```bash
./manage.py faststream
```