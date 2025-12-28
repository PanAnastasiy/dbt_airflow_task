import snowflake.connector
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from utils.alerts import TelegramAlert
from utils.consts import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from utils.snowflake_config import SnowflakeEnvConfig

# Импорты твоих утилит (пути должны совпадать с твоей структурой)
# Если запускаешь в Docker через этот гайд, то импорты будут такими:

tg_notifier = TelegramAlert()
sf_config = SnowflakeEnvConfig()

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': tg_notifier.send,
}

def decide_load_type(**kwargs):
    """
    Подключается к Snowflake и проверяет наличие таблиц.
    Возвращает task_id следующего шага.
    """
    print(f"Checking schema: {sf_config.schema} in DB: {sf_config.database}")

    conn = snowflake.connector.connect(
        user=sf_config.user,
        password=sf_config.password,
        account=sf_config.account,
        warehouse=sf_config.warehouse,
        database=sf_config.database,
        schema=sf_config.schema,
        role=sf_config.role
    )

    try:
        cur = conn.cursor()
        # Считаем количество таблиц в целевой схеме
        query = f"""
            SELECT count(*) 
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '{sf_config.schema.upper()}'
            AND TABLE_CATALOG = '{sf_config.database.upper()}'
        """
        cur.execute(query)
        table_count = cur.fetchone()[0]

        print(f"Tables found: {table_count}")

        if table_count > 0:
            return 'incremental_run'
        else:
            return 'initial_load_run'

    finally:
        conn.close()

with DAG('smart_dbt_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # 1. Ветвление: Проверка состояния базы
    branch_task = BranchPythonOperator(
        task_id='check_snowflake_state',
        python_callable=decide_load_type
    )

    # 2. Ветка INITIAL (данных нет -> Full Refresh)
    initial_load = BashOperator(
        task_id='initial_load_run',
        # --full-refresh пересоздаст таблицы с нуля
        bash_command=f'dbt build --full-refresh --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR
    )

    # 3. Ветка INCREMENTAL (данные есть -> Обычный build)
    incremental_load = BashOperator(
        task_id='incremental_run',
        bash_command=f'dbt build --profiles-dir {DBT_PROFILES_DIR}',
        cwd=DBT_PROJECT_DIR
    )

    # 4. Финальная точка (чтобы отправить алерт об успехе)
    final_success = BashOperator(
        task_id='pipeline_success',
        bash_command='echo "DBT Load Completed Successfully"',
        trigger_rule=TriggerRule.ONE_SUCCESS, # Запустится, если или Initial или Incremental прошел успешно
        on_success_callback=tg_notifier.send
    )

    # Строим граф
    branch_task >> [initial_load, incremental_load]
    initial_load >> final_success
    incremental_load >> final_success