# airflow/Dockerfile

FROM apache/airflow:2.10.2-python3.12

USER root

# 1. Устанавливаем uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# 2. Устанавливаем системные утилиты (git нужен для dbt deps)
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow

# 3. Копируем файлы определения зависимостей из корня монорепозитория
# Теперь копируем только корневые файлы
COPY --chown=airflow:root pyproject.toml uv.lock /opt/airflow/

# 4. Устанавливаем зависимости
# uv sync установит ВСЕ зависимости, определенные в корневом pyproject.toml
RUN uv sync --frozen --all-extras

# 5. Настраиваем переменные окружения для dbt
ENV DBT_PROFILES_DIR=/opt/airflow/dbt_project
ENV DBT_PROJECT_DIR=/opt/airflow/dbt_project