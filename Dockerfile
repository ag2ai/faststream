FROM python:3.10

ENV PYTHONUNBUFFERED=1

COPY . /src

WORKDIR /src

RUN pip install --upgrade pip && pip install -e ".[dev]"

