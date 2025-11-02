---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 3
---

# Documentation

## How to help

You will be of invaluable help if you contribute to the documentation.

Such a contribution can be:

* Indications of inaccuracies, errors, typos
* Suggestions for editing specific sections
* Making additions

You can report all this in [discussions](https://github.com/ag2ai/faststream/discussions){.external-link target="_blank"} on GitHub, start [issue](https://github.com/ag2ai/faststream/issues){.external-link target="_blank"}, or write about it in our [discord](https://discord.gg/qFm6aSqq59){.external-link target="_blank"} group.

!!! note
    Special thanks to those who are ready to offer help with the case and help in **developing documentation**, as well as translating it into **other languages**.

## How to get started

To develop the documentation, you don't even need to install the entire **FastStream** project as a whole.

1. [Install justfile on your system](https://just.systems/man/en/prerequisites.html)

    View all available commands:

    ```bash
    just
    ```

2. [Install uv on your system](https://docs.astral.sh/uv/getting-started/installation/)
3. Clone the project repository
4. Start the local documentation server
    ```bash
    just docs-serve
    ```
    For a full build with all dependencies and extended processing, use:
    ```bash
    just docs-serve --full
    ```

Now all changes in the documentation files will be reflected on your local version of the site.
After making all the changes, you can issue a `PR` with them - and we will gladly accept it!


## Guidelines

### Links in documentation

- External links need to mark `{.external-link target="_blank"}`

    (e.g [**Propan**](https://github.com/lancetnik/propan){.external-link target="_blank"} - `[**Propan**](https://github.com/lancetnik/propan){.external-link target="_blank"}`)

- Internal links need to mark `{.internal-link}`

    (e.g [contribution page](/getting-started/contributing/contributing){.internal-link} - `[contribution page](/getting-started/contributing/contributing){.internal-link}`)

- A lot of links going in a row doesn't need to mark both `{.external_link}` and `{.internal_link}`. In this case use only `{target="_blank"}` for external links

    (e.g [JSON](https://www.json.org/json-en.html){target="_blank"}, [MessagePack](https://msgpack.org/){target="_blank"}, [YAML](https://yaml.org/){target="_blank"}, and [TOML](https://toml.io/en/){target="_blank"} - `[JSON](https://www.json.org/json-en.html){target="_blank"}, [MessagePack](https://msgpack.org/){target="_blank"}, [YAML](https://yaml.org/){target="_blank"}, and [TOML](https://toml.io/en/){target="_blank"}`)
