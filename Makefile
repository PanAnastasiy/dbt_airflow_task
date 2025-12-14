
CONTAINER_NAME = airflow
DBT_PROJECT_DIR = /opt/airflow/dbt_customer_project
LOCAL_DBT_DIR = ./core/dbt_customer_project

.PHONY: help up down restart shell logs dbt-check dbt-seed dbt-run dbt-test dbt-docs clean lint fix setup

.DEFAULT_GOAL := help

help:
	@echo "Управление проектом dbt + Airflow Data Vault"
	@echo "------------------------------------------------"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup:
	@if [ ! -f .env ]; then \
		echo "Копирую .env.example в .env..."; \
		cp .env.example .env; \
		echo "✅ Файл .env создан! Заполни его своими данными."; \
	else \
		echo "ℹ️  Файл .env уже существует."; \
	fi

up: setup
	docker-compose up -d --build
	@echo "⏳ Подожди немного, пока Airflow инициализируется..."
	@echo "🔗 UI доступен по адресу: http://localhost:8080"

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f airflow

shell:
	docker-compose exec $(CONTAINER_NAME) /bin/bash

dbt-check:
	docker-compose exec $(CONTAINER_NAME) dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

dbt-seed:
	docker-compose exec $(CONTAINER_NAME) dbt seed --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

dbt-run:
	docker-compose exec $(CONTAINER_NAME) dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

dbt-test:
	docker-compose exec $(CONTAINER_NAME) dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir .

dbt-docs:
	docker-compose exec $(CONTAINER_NAME) dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir .
	@echo "Документация сгенерирована в папке target/"

lint:
	uv run sqlfluff lint $(LOCAL_DBT_DIR) --ignore E501 --dialect snowflake

fix:
	uv run sqlfluff fix $(LOCAL_DBT_DIR) --ignore E501 --dialect snowflake

clean:
	rm -rf $(LOCAL_DBT_DIR)/target
	rm -rf $(LOCAL_DBT_DIR)/dbt_packages
	rm -rf logs/*
	@echo "Мусор убран."