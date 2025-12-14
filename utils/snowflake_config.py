import os
from dotenv import load_dotenv


class SnowflakeEnvConfig:

    REQUIRED_VARS = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA",
        "SNOWFLAKE_TARGET",
    ]

    def __init__(self, env_path: str | None = None):
        load_dotenv(env_path)
        self._validate()

    def _get(self, key: str) -> str:
        return os.getenv(key)

    def _validate(self):
        missing = [v for v in self.REQUIRED_VARS if not os.getenv(v)]
        if missing:
            raise RuntimeError(
                f"Missing required Snowflake env vars: {', '.join(missing)}"
            )
    @property
    def account(self) -> str:
        return self._get("SNOWFLAKE_ACCOUNT")

    @property
    def user(self) -> str:
        return self._get("SNOWFLAKE_USER")

    @property
    def password(self) -> str:
        return self._get("SNOWFLAKE_PASSWORD")

    @property
    def role(self) -> str:
        return self._get("SNOWFLAKE_ROLE")

    @property
    def warehouse(self) -> str:
        return self._get("SNOWFLAKE_WAREHOUSE")

    @property
    def database(self) -> str:
        return self._get("SNOWFLAKE_DATABASE")

    @property
    def schema(self) -> str:
        return self._get("SNOWFLAKE_SCHEMA")

    @property
    def target(self) -> str:
        return self._get("SNOWFLAKE_TARGET")
