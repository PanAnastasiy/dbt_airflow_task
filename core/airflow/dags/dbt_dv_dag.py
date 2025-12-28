from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup

from utils.consts import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from utils.alerts import TelegramAlert

tg_notifier = TelegramAlert()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': tg_notifier.send,
    'on_success_callback': tg_notifier.send
}

with DAG('dbt_data_vault_master', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        bash_command=f'dbt seed --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR
    )

    with TaskGroup("raw_vault_layer") as raw_group:
        dbt_hubs = BashOperator(
            task_id='run_hubs',
            bash_command=f'dbt run --models tag:hubs --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

        dbt_links = BashOperator(
            task_id='run_links',
            bash_command=f'dbt run --models tag:links --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

        dbt_sats = BashOperator(
            task_id='run_sats',
            bash_command=f'dbt run --models tag:sats --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

        dbt_hubs >> dbt_links >> dbt_sats

    with TaskGroup("business_vault_layer") as bv_group:
        dbt_pit = BashOperator(
            task_id='run_pit',
            bash_command=f'dbt run --models pit_customer --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

        dbt_bv_sats = BashOperator(
            task_id='run_bv_sats',
            bash_command=f'dbt run --models sat_bv_customer_combined --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

        dbt_pit >> dbt_bv_sats

    with TaskGroup("marts_layer") as marts_group:
        dbt_marts = BashOperator(
            task_id='run_marts',
            bash_command=f'dbt run --models marts --profiles-dir {DBT_PROFILES_DIR}',
            cwd=DBT_PROJECT_DIR
        )

    dbt_seed >> raw_group >> bv_group >> marts_group
