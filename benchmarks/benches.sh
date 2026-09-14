#!/bin/bash

set -e

# Цвета для вывода
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${YELLOW}🚀 Запуск бенчмарка FastStream...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo "Параметры:"
echo "  - CPU: 0.5 ядра"
echo "  - Память: 256 МБ"
echo "  - Python: 3.12-slim"
echo "  - Профили: rabbit, kafka, confluent, nats, redis, otel, prometheus"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""


# Проверка requirements файла
if [ ! -f "requirements-bench.txt" ]; then
    echo -e "${RED}❌ Файл requirements-bench.txt не найден${NC}"
    echo "Создаю requirements-bench.txt..."
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

# Запуск бенчмарка в Docker
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
  
  echo '📦 Установка зависимостей бенчмарка...'
  pip install --no-cache-dir -r /requirements-bench.txt
  
  echo ''
  echo '✅ Все зависимости установлены успешно'
  echo -e '\033[0;34m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m'
  echo ''
  echo '⏱️  Запуск бенчмарка...'
  echo ''
  
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
    exit $EXIT_CODE
fi