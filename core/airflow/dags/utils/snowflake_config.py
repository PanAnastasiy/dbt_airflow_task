import os

from dotenv import load_dotenv

class SnowflakeEnvConfig:

    def __init__(self, env_path: str | None = None):
        load_dotenv(env_path)
        self.account   = self._get("SNOWFLAKE_ACCOUNT")
        self.user      = self._get("SNOWFLAKE_USER")
        self.password  = self._get("SNOWFLAKE_PASSWORD")
        self.role      = self._get("SNOWFLAKE_ROLE")
        self.warehouse = self._get("SNOWFLAKE_WAREHOUSE")
        self.database  = self._get("SNOWFLAKE_DATABASE")
        self.schema    = self._get("SNOWFLAKE_SCHEMA")
        self.target    = self._get("SNOWFLAKE_TARGET")

    def _get(self, key: str) -> str:

        value = os.getenv(key)
        if not value:
            raise ValueError(f"Missing required Snowflake env var: {key}")
        return value
