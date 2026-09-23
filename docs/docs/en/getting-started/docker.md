---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
description: >-
  Use the official FastStream Docker images from GitHub Container Registry.
search:
  boost: 8
---

# Docker Images

FastStream publishes Python-versioned images to the GitHub Container Registry:

```text
ghcr.io/ag2ai/faststream:<version>-<python-version>
```

For example:

```text
ghcr.io/ag2ai/faststream:0.7.6-3.12
```

The image contains the FastStream CLI and is intended to be used as a base image for your application. Pull a release image with:

```bash
docker pull ghcr.io/ag2ai/faststream:0.7.6-3.12
```

## Running an Application

The default command is equivalent to:

```bash
faststream run serve:app --workers 2
```

Run an application with the default command:

```bash
docker run --rm \
  --name faststream-app \
  --env-file .env \
  ghcr.io/ag2ai/faststream:0.7.6-3.12
```

The application module must be available inside the container. A typical application image can be defined as:

```dockerfile
FROM ghcr.io/ag2ai/faststream:0.7.6-3.12

COPY . /app
```

## Passing CLI Arguments

Arguments after the image name replace the default command while keeping the `faststream` entrypoint. Use this to change the number of workers:

```bash
docker run --rm \
  --name faststream-app \
  --env-file .env \
  ghcr.io/ag2ai/faststream:0.7.6-3.12 \
  run serve:app --workers 4
```

You can pass any supported `faststream run` option in the same way:

```bash
docker run --rm \
  ghcr.io/ag2ai/faststream:0.7.6-3.12 \
  run serve:app --workers 2 --log-level debug
```

To inspect the available CLI commands and options:

```bash
docker run --rm ghcr.io/ag2ai/faststream:0.7.6-3.12 --help
docker run --rm ghcr.io/ag2ai/faststream:0.7.6-3.12 run --help
```
