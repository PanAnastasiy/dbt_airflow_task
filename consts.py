from pathlib import Path

DBT_ROOT_PATH = Path("/opt/airflow/dbt_customer_project")
# в конфиге дага хочет Path, а в качестве аргумента путь, немного терпимо xD
DBT_PROJECT_DIR = '/opt/airflow/dbt_customer_project'
DBT_PROFILES_DIR = '.'
