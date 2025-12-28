from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from utils.telegram_alerts import TelegramAlert
from utils.consts import DBT_PROFILES_DIR, DBT_PROJECT_DIR

tg_notifier = TelegramAlert()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': tg_notifier.send,
}

with DAG(
    'cleanup_snowflake_schema', default_args=default_args, schedule_interval=None, catchup=False
) as dag:

    clean_local = BashOperator(
        task_id='dbt_clean_local',
        bash_command=f'dbt clean --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR,
    )

    drop_schema_snowflake = BashOperator(
        task_id='drop_snowflake_schema',
        bash_command=f'dbt run-operation drop_my_schema --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR,
        on_success_callback=tg_notifier.send,
    )

    clean_local >> drop_schema_snowflake
