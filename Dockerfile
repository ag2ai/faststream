ARG PYTHON_VERSION=3.8

FROM python:$PYTHON_VERSION

ENV PYTHONUNBUFFERED=1

COPY . /src

WORKDIR /src

RUN pip install --upgrade pip && pip install -e ".[dev]"
