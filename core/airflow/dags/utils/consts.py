from pathlib import Path

DBT_ROOT_PATH = Path("/opt/airflow/dbt_customer_project")
# DAG config expects Path, but argument is a string path (acceptable workaround)
DBT_PROJECT_DIR = '/opt/airflow/dbt_customer_project'
DBT_PROFILES_DIR = '../../../..'
