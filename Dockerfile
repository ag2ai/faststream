ARG PYTHON_VERSION=3.10

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1

COPY . /src

WORKDIR /src

RUN uv sync --group dev
