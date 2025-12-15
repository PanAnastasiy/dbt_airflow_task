CONTAINER_NAME = airflow
DBT_PROJECT_DIR = /opt/airflow/dbt_customer_project
LOCAL_DBT_DIR = core/dbt_customer_project

.DEFAULT_GOAL := help

.PHONY: help up down restart logs shell install \
        dbt-check dbt-seed dbt-run dbt-test dbt-docs

help:
	@echo "dbt + Airflow Data Vault Project"
	@echo "--------------------------------"
	@echo "make up           Start Airflow environment"
	@echo "make down         Stop containers"
	@echo "make restart      Restart environment"
	@echo "make install      Full dbt pipeline"
	@echo "make logs         Show Airflow logs"
	@echo "make shell        Open Airflow container shell"
	@echo "make dbt-run      Run dbt models"
	@echo ""
	@echo "NOTE:"
	@echo "Make sure .env file exists before running 'make up'"

up:
	docker-compose up -d --build
	@echo "Airflow UI → http://localhost:8080"

down:
	docker-compose down

restart: down up

logs:
	docker-compose logs -f $(CONTAINER_NAME)

shell:
	docker-compose exec $(CONTAINER_NAME) /bin/bash

install:
	$(MAKE) up
	$(MAKE) dbt-check
	$(MAKE) dbt-seed
	$(MAKE) dbt-run
	$(MAKE) dbt-test
	$(MAKE) dbt-docs
	@echo "Pipeline finished successfully"

dbt-check:
	docker-compose exec $(CONTAINER_NAME) \
	dbt debug --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-seed:
	docker-compose exec $(CONTAINER_NAME) \
	dbt seed --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-run:
	docker-compose exec $(CONTAINER_NAME) \
	dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-test:
	docker-compose exec $(CONTAINER_NAME) \
	dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

dbt-docs:
	docker-compose exec $(CONTAINER_NAME) \
	dbt docs generate --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROJECT_DIR)

