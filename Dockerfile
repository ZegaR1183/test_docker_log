FROM python:3.12-slim

LABEL maintainer=Evgeniy_Raikhin

WORKDIR /app

# Установка системных зависимостей
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Копирование зависимостей
COPY requirements.txt .

# Установка Python зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование исходного кода
COPY . .

# Создаём необходимые директории
RUN mkdir -p ./temp ./output_files ./input_file ./logs

# Установка утилиты для ожидания
RUN pip install psycopg2-binary

# Создаём скрипт ожидания БД
COPY wait-for-postgres.sh /wait-for-postgres.sh
RUN chmod +x /wait-for-postgres.sh

# По умолчанию ждём БД и запускаем скрипт
CMD ["/wait-for-postgres.sh", "python", "log.py"]