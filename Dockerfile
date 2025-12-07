ARG PYTHON_VERSION=3.10

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1

COPY ./pyproject.toml ./README.md ./LICENSE /src/
COPY ./faststream/__init__.py /src/faststream/__init__.py

WORKDIR /src

RUN uv sync --group dev

ENV PATH="/src/.venv/bin:$PATH"
