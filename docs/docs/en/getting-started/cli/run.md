## Running the Project

The primary **FastStream CLI** command is `faststream run`, which launches your application.

```shell
faststream run --help
```

```{ .console .no-copy }
Options:
  --workers -w INTEGER              Run [workers] applications with process spawning.
                                    [env var: FASTSTREAM_WORKERS]
  --app-dir TEXT                    Look for APP in the specified directory,
                                    by adding this to the PYTHONPATH.
                                    [env var: FASTSTREAM_APP_DIR]
                                    [default: .]
  --factory -f                      Treat APP as an application factory.
  --reload -r                       Restart app at directory files changes.
  --extension,--ext,--reload-extension,--reload-ex TEXT
                                    List of file extensions to watch by.
  --log-level -l [critical|fatal|error|warning|warn|info|debug|notset]
                                    Set selected level for FastStream and brokers logger objects.
                                    [env var: FASTSTREAM_LOG_LEVEL]
  --log-config PATH                 Set file to configure logging.
                                    Support `.json`, `.yaml`, `.yml`, `.toml` extensions.
  --help                            Show this message and exit.
```
{ data-search-exclude }

### Multiprocessing Scaling

**FastStream** allows you to scale application right from the command line by running you application in the Process pool.

Just set the `--workers` option to scale your application:

```shell
faststream run serve:app --workers 2
```

```{ .console .no-copy }
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


```{ .console .no-copy }
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

```{ .console .no-copy }
INFO     - FastStream app starting...
INFO     - test |            - `Handle` waiting for messages
INFO     - FastStream app started successfully! To exit press CTRL+C
```
{ data-search-exclude }

{! includes/en/env-context.md !}

!!! note
    Note that the `env` parameter was passed to the `setup` function directly from the command line

All passed values can be of type `#!python bool`, `#!python str` or `#!python list[str]`.

In this case, the flags will be interpreted as follows:

```{ .console .no-copy }
faststream run app:app --flag             # flag = True
faststream run app:app --no-flag          # flag = False
faststream run app:app --my-flag          # my_flag = True
faststream run app:app --key value        # key = "value"
faststream run app:app --key 1 2          # key = ["1", "2"]
faststream run app:app --key 1 --key 2    # key = ["1", "2"]
```
{ data-search-exclude }

You can use them both individually and together in unlimited quantities.


### Uvicorn options
All [*Uvicorn options*](https://www.uvicorn.org/#command-line-options){.external-link target="_blank"} (except options that exist in **FastStream**) are forwarded directly to [**Uvicorn**](https://www.uvicorn.org/){.external-link target="_blank"} when you run app as ASGI.

!!! note
    Running your app as ASGI is optional. For details, see [*ASGI Support*](../asgi.md)

```shell
faststream run app:app --host 0.0.0.0 --port 8080
```
