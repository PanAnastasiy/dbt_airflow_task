from pathlib import Path

# DAG config expects Path, but argument is a string path (acceptable workaround)
DBT_ROOT_PATH = Path("/opt/airflow/dbt_customer_project")
DBT_PROJECT_DIR = "/opt/airflow/dbt_customer_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_customer_project"
LOG_FILE_PATH = "/opt/airflow/logs/app.log"
