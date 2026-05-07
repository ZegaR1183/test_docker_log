#!/bin/bash

# Ожидание доступности PostgreSQL

host="$1"
shift
cmd="$@"

export PYTHONPATH="/app:$PYTHONPATH"

until python -c "
import os
import sys
try:
    import psycopg2
    conn = psycopg2.connect(
        host='${DB_HOST}',
        port='${DB_PORT}',
        database='${POSTGRES_DB}',
        user='${POSTGRES_USER}',
        password='${POSTGRES_PASSWORD}'
    )
    conn.close()
    print('PostgreSQL доступен — продолжаем...')
    sys.exit(0)
except Exception as e:
    print('Ожидание PostgreSQL... Ошибка подключения:', e)
    sys.exit(1)
" >/dev/null 2>&1; do
  echo '⏳ Ожидание PostgreSQL... Спит 2 секунды'
  sleep 2
done

echo "✅ PostgreSQL запущен и готов к работе"

# Запуск основной команды
exec $cmd