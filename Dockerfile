FROM apache/airflow:2.10.3-python3.10

USER root

RUN apt-get update \
    && apt-get install -y git curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir uv

RUN uv pip install --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    astronomer-cosmos==1.7.0 \
    apache-airflow-providers-snowflake \
    loguru \
    python-dotenv \
    requests

COPY --chown=airflow:airflow core/airflow/dags /opt/airflow/dags
COPY --chown=airflow:airflow core/dbt_customer_project /opt/airflow/dbt_customer_project

COPY --chown=airflow:airflow entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
