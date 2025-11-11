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

### Code Examples

When contributing code examples to the documentation, follow these rules to keep examples maintainable and testable:

- **Write Python files in `docs/docs_src/`**: Place all example Python code in the `docs/docs_src/` directory. Organize files by topic and subtopic, e.g., `docs/docs_src/getting_started/basic.py` for a basic FastStream app example.

    See below how to use this code examples in the documentation MD file:

    ```
        ````python linenums="1" hl_lines="10 20"
        \{!> docs_src/getting_started/publishing/kafka/broker.py !}
        ```
    ```

    if yours file has more than 3 a lines, use the keyword `linenums`. If you need to highlight certain lines to record line numbers using a `space` separator to `hl_lines` keyword.

- **Add tests in `tests/docs/`**: For every example file in `docs/docs_src/`, create corresponding test files in `tests/docs/`. These tests verify that the examples run correctly and behave as expected. Use pytest, mark tests appropriately (e.g., with broker-specific marks if needed), and ensure they pass before submitting changes.

- **Include examples using [`mdx_include`](https://github.com/neurobin/mdx_include){.external-link target="_blank"}**: To embed the code examples directly into Markdown documentation files, use the `mdx_include` Markdown extension. This allows including the content of Python files (local or remote) at arbitrary positions in the docs.

This approach ensures that code examples are version-controlled, testable, and easily reusable across multiple documentation pages.
