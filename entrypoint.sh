#!/bin/bash

set -e

echo "Starting Airflow with DBT support..."


export DBT_PROJECT_DIR=/opt/airflow/dbt_customer_project
export DBT_PROFILES_DIR=/opt/airflow/dbt_customer_project

airflow db upgrade

if [[ "$_AIRFLOW_WWW_USER_CREATE" == "true" ]]; then
    airflow users create \
        --username "${_AIRFLOW_WWW_USER_USERNAME}" \
        --password "${_AIRFLOW_WWW_USER_PASSWORD}" \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com || true
fi

exec "$@"
