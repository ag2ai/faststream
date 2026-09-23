ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY ./pyproject.toml ./README.md ./LICENSE /tmp/faststream/
COPY ./faststream /tmp/faststream/faststream

ARG FASTSTREAM_EXTRAS=cli
RUN pip install --no-cache-dir "/tmp/faststream[${FASTSTREAM_EXTRAS}]" \
	&& rm -rf /tmp/faststream

ENTRYPOINT ["faststream"]

CMD ["run", "serve:app", "--workers", "2"]