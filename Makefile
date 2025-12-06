.PHONY: up down restart build dbt-check lint fix


up:
	docker-compose up -d --build

# Остановить всё
down:
	docker-compose down

# Перезапуск (если что-то зависло)
restart: down up

# Зайти внутрь контейнера Airflow (для дебага)
shell:
	docker-compose exec airflow-scheduler /bin/bash

# Проверка dbt соединения изнутри контейнера
dbt-check:
	docker-compose exec airflow-scheduler dbt debug --project-dir /opt/airflow/dbt_project

# Команда для запуска линтера SQL
lint:
	# uv run позволяет запустить команду в виртуальном окружении,
	# которое определено в pyproject.toml
	uv run sqlfluff lint --ignore E501 --project-dir .

# Команда для автоформатирования SQL
fix:
	uv run sqlfluff fix --project-dir .