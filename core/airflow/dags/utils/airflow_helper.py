import snowflake.connector
from loguru import logger

from .snowflake_config import SnowflakeEnvConfig

sf_config = SnowflakeEnvConfig()


def decide_load_type(**context) -> str:
    logger.info(
        "Checking Snowflake schema state",
        database=sf_config.database,
        schema=sf_config.schema,
    )

    conn = snowflake.connector.connect(
        user=sf_config.user,
        password=sf_config.password,
        account=sf_config.account,
        warehouse=sf_config.warehouse,
        database=sf_config.database,
        schema=sf_config.schema,
        role=sf_config.role,
    )

    try:
        cur = conn.cursor()
        query = """
                SELECT count(*)
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %(schema)s
                  AND TABLE_CATALOG = %(database)s \
                """

        cur.execute(
            query,
            {
                "schema": sf_config.schema.upper(),
                "database": sf_config.database.upper(),
            },
        )

        table_count = cur.fetchone()[0]

        logger.info(
            "Snowflake tables count fetched",
            tables=table_count,
        )

        if table_count > 0:
            logger.info("Incremental load selected")
            return "incremental_run"

        logger.info("Initial full-refresh load selected")
        return "initial_load_run"

    except Exception as exc:
        logger.exception("Failed to inspect Snowflake schema", error=str(exc))
        raise

    finally:
        conn.close()
