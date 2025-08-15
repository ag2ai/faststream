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
pip install 'faststream[cli]'
```

After installation, you can check which commands are available by executing:

```shell
faststream --help
```

```{ .console .no-copy }
Usage: faststream [OPTIONS] COMMAND [ARGS]...

  Generate, run and manage FastStream apps to greater development experience

Options:
  --version             -v        Show current platform, python and FastStream
                                  version.
  --install-completion            Install completion for the current shell.
  --show-completion               Show completion for the current shell, to
                                  copy it or customize the installation.
  --help                          Show this message and exit.

Commands:
  run       Run [MODULE:APP] FastStream application.
  publish   Publish a message using the specified broker
            in a FastStream application.
  docs      Documentations commands
```
{ data-search-exclude }
