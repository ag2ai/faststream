#!/usr/bin/env bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GREEN='\033[0;32m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

COMPOSE_FILE="docker-compose.yaml"

cleanup() {
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo "Stopping and removing benchmark containers..."
    docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans
}
trap cleanup EXIT

if [ ! -f "requirements-bench.txt" ]; then
    cat > requirements-bench.txt << 'EOF'
faststream[rabbit,kafka,confluent,nats,redis,otel,prometheus]
aiokafka>=0.9,<0.15
nats-py>=2.12.0,<=3.0.0
aio-pika>=9,<11
redis>=5.0.0,<9.0.0
asyncpg>=0.30.0,<0.31.0
msgspec
fast-depends[msgspec]>=3.0.0
opentelemetry-exporter-otlp-proto-http>=1.24.0,<2.0.0
opentelemetry-semantic-conventions>=0.45b0
prometheus-client>=0.20.0,<0.30.0
pytest==9.0.3
pytest-asyncio==1.3.0
psutil==7.2.2
EOF
    echo -e "${GREEN}✅ requirements-bench.txt создан${NC}"
    echo ""
fi

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "Starting benchmark dependencies (kafka, postgres, otel-collector)..."
docker compose -f "$COMPOSE_FILE" up -d --wait
COMPOSE_UP_CODE=$?

if [ $COMPOSE_UP_CODE -ne 0 ]; then
    echo -e "${RED}❌ Не удалось поднять зависимости (docker compose up), код: ${COMPOSE_UP_CODE}${NC}"
    exit $COMPOSE_UP_CODE
fi

docker run --rm \
  --cpus="0.5" \
  --memory="256m" \
  -v "$PWD":/benchmarks \
  -v "$PWD/requirements-bench.txt":/requirements-bench.txt \
  -w /benchmarks \
  --network=host \
  python:3.12-slim \
  /bin/bash -c "
  set -e

  pip install --no-cache-dir -r /requirements-bench.txt

  python bench.py
  "

EXIT_CODE=$?

echo ""
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${GREEN}✅ Бенчмарк завершён успешно${NC}"
else
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}❌ Ошибка при выполнении бенчмарка (код: $EXIT_CODE)${NC}"
fi

exit $EXIT_CODE
