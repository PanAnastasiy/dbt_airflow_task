from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from utils.airflow_helper import decide_load_type
from utils.telegram_alerts import TelegramAlert
from utils.consts import DBT_PROFILES_DIR, DBT_PROJECT_DIR

tg_notifier = TelegramAlert()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': tg_notifier.send,
}

with DAG(
        'smart_dbt_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False
) as dag:

    branch_task = BranchPythonOperator(
        task_id='check_snowflake_state',
        python_callable=decide_load_type,
    )

    initial_load = BashOperator(
        task_id='initial_load_run',
        bash_command=f'dbt build --full-refresh --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR,
    )

    incremental_load = BashOperator(
        task_id='incremental_run',
        bash_command=f'dbt build --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR,
    )

    final_success = BashOperator(
        task_id='pipeline_success',
        bash_command='echo "DBT Load Completed Successfully"',
        trigger_rule=TriggerRule.ONE_SUCCESS,
        on_success_callback=tg_notifier.send,
    )

    branch_task >> [initial_load, incremental_load]
    initial_load >> final_success
    incremental_load >> final_success
