ARG PYTHON_VERSION=3.10
ARG MQ_VERSION=9.4.5.0

FROM python:$PYTHON_VERSION
COPY --from=ghcr.io/astral-sh/uv:0.7.13 /uv /uvx /bin/

ENV PYTHONUNBUFFERED=1
ENV MQ_FILE_PATH=/opt/mqm
ENV LD_LIBRARY_PATH=/opt/mqm/lib64

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/mqm \
    && curl -fsSL "https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/${MQ_VERSION}-IBM-MQC-Redist-LinuxX64.tar.gz" -o /tmp/ibmmq-redist.tar.gz \
    && tar -zxf /tmp/ibmmq-redist.tar.gz -C /opt/mqm \
    && rm -f /tmp/ibmmq-redist.tar.gz \
    && /opt/mqm/bin/genmqpkg.sh -b /opt/mqm

COPY ./pyproject.toml ./README.md ./LICENSE /src/
COPY ./faststream/__init__.py /src/faststream/__init__.py

WORKDIR /src

RUN uv sync --group dev

ENV PATH="/src/.venv/bin:$PATH"
