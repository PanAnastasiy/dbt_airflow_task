from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

from utils.consts import DBT_ROOT_PATH
from utils.snowflake_config import SnowflakeEnvConfig

sf_config = SnowflakeEnvConfig()

profile_config = ProfileConfig(
    profile_name="dbt_airflow_project",
    target_name=sf_config.target,
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_default",
        profile_args={
            "database": sf_config.database,
            "schema": sf_config.schema,
            "warehouse": sf_config.warehouse,
            "role": sf_config.role,
        },
    )
)

my_dbt_dag = DbtDag(
    project_config=ProjectConfig(
        dbt_project_path=DBT_ROOT_PATH,
    ),
    profile_config=profile_config,
    execution_config=ExecutionConfig(
        dbt_executable_path="dbt",
    ),
    dag_id="snowflake_data_vault",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["dbt", "snowflake", "vault"],
)
