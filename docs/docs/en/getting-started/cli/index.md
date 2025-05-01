---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# CLI

**FastStream** has its own built-in **CLI** tool for your maximum comfort as a developer.

!!! quote ""
    Thanks to [*typer*](https://typer.tiangolo.com/){.external-link target="_blank"} and [*watchfiles*](https://watchfiles.helpmanual.io/){.external-link target="_blank"}. Their work is the basis of this tool.

## Installation

To install the **FastStream CLI**, you need to run the following command:

```shell
pip install faststream[cli]
```

After installation, you can check which commands are available by executing:

```shell
faststream --help
```

```{ .shell .no-copy }
Usage: faststream [OPTIONS] COMMAND [ARGS]...

  Generate, run and manage FastStream apps to greater development experience

Options:
  -v, --version                   Show current platform, python and FastStream
                                  version
  --install-completion [bash|zsh|fish|powershell|pwsh]
                                  Install completion for the specified shell.
  --show-completion [bash|zsh|fish|powershell|pwsh]
                                  Show completion for the specified shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.

Commands:
  docs  AsyncAPI schema commands
  run   Run [MODULE:APP] FastStream application
```
{ data-search-exclude }

## Running the Project

### Multiprocessing Scaling

**FastStream** allows you to scale application right from the command line by running you application in the Process pool.

Just set the `--worker` option to scale your application:

```shell
faststream run serve:app --workers 2
```

```{ .shell .no-copy }
INFO     - Started parent process [7591]
INFO     - Started child process [7593]
INFO     - Started child process [7594]
INFO     - test |            - `Handle` waiting for messages
INFO     - test |            - `Handle` waiting for messages
```
{ data-search-exclude }

### Hot Reload

Thanks to [*watchfiles*](https://watchfiles.helpmanual.io/){.external-link target="_blank"}, written in *Rust*, you can
work with your project easily. Edit the code as much as you like - the new version has already been launched and is waiting for your requests!

```shell
faststream run serve:app --reload
```

!!! tip
    Please, install [`watchfiles`](https://github.com/samuelcolvin/watchfiles){.external-link target="_blank"} if you want to use `--reload` feature

    ```shell
    pip install watchfiles
    ```


```{ .shell .no-copy }
INFO     - Started reloader process [7902] using WatchFiles
INFO     - FastStream app starting...
INFO     - test |            - `Handle` waiting for messages
INFO     - FastStream app started successfully! To exit press CTRL+C
```
{ data-search-exclude }

By default **FastStream** watches for `.py` file changes, but you can specify an extra file extensions to watch by (your config files as an example)

```shell
faststream run serve:app --reload  --reload-ext .yml --realod-ext .yaml
```

### Environment Management

You can pass any custom flags and launch options to the **FastStream CLI** even without first registering them. Just use them when launching the application - and they will be right in your environment.

Use this option to select environment files, configure logging, or at your discretion.

For example, we will pass the *.env* file to the context of our application:

```shell
faststream run serve:app --env=.env.dev
```

```{ .shell .no-copy }
INFO     - FastStream app starting...
INFO     - test |            - `Handle` waiting for messages
INFO     - FastStream app started successfully! To exit press CTRL+C
```
{ data-search-exclude }

=== "AIOKafka"
    ```python linenums="1" hl_lines="14-16"
    {!> docs_src/getting_started/cli/kafka_context.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="14-16"
    {!> docs_src/getting_started/cli/confluent_context.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="14-16"
    {!> docs_src/getting_started/cli/rabbit_context.py [ln:1-10.53,11-] !}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="14-16"
    {!> docs_src/getting_started/cli/nats_context.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="14-16"
    {!> docs_src/getting_started/cli/redis_context.py!}
    ```

!!! note
    Note that the `env` parameter was passed to the `setup` function directly from the command line

All passed values can be of type `#!python bool`, `#!python str` or `#!python list[str]`.

In this case, the flags will be interpreted as follows:

```{ .shell .no-copy }
faststream run app:app --flag             # flag = True
faststream run app:app --no-flag          # flag = False
faststream run app:app --my-flag          # my_flag = True
faststream run app:app --key value        # key = "value"
faststream run app:app --key 1 2          # key = ["1", "2"]
faststream run app:app --key 1 --key 2    # key = ["1", "2"]
```
{ data-search-exclude }

You can use them both individually and together in unlimited quantities.

## AsyncAPI Schema

Also, the **FastStream CLI** allows you to work with the **AsyncAPI** schema in a simple way.

You are able to generate `.json` or `.yaml` files by your application code or host **HTML** representation directly:

```shell
faststream docs --help
```

```{ .shell .no-copy }
Usage: faststream docs [OPTIONS] COMMAND [ARGS]...

  AsyncAPI schema commands

Options:
  --help  Show this message and exit.

Commands:
  gen    Generate project AsyncAPI schema
  serve  Serve project AsyncAPI schema
```
{ data-search-exclude }

To learn more about the commands above, please visit [**AsyncAPI export**](../asyncapi/export.md){.internal-link} and [**AsyncAPI hosting**](../asyncapi/hosting.md){.internal-link}.
