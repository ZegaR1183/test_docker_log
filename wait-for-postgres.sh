#!/bin/bash
set -e

# Ожидание доступности PostgreSQL через Python/psycopg2

echo "Ожидание PostgreSQL... Проверка подключения"

max_retries=30
retry_count=0

# Команда для запуска передаётся в CMD (например: python log.py)
if [ $# -eq 0 ]; then
  echo "Не передана команда для выполнения."
  exit 1
fi

until python -c "
import os
import sys
import psycopg2

try:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST', 'postgres'),
        port=int(os.getenv('POSTGRES_PORT', 5432)),
        database=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD')
    )
    conn.close()
    print('Подключение к PostgreSQL успешно.')
    sys.exit(0)
except Exception as e:
    print(f'⏳ Ожидание PostgreSQL... Ошибка: {e}')
    sys.exit(1)
"; do
  retry_count=$((retry_count + 1))
  if [ $retry_count -ge $max_retries ]; then
    echo "Превышено количество попыток подключения к PostgreSQL."
    exit 1
  fi
  echo "💤 Повторная попытка через 2 секунды... ($retry_count/$max_retries)"
  sleep 2
done

echo "Запуск основной команды: $*"
exec "$@"