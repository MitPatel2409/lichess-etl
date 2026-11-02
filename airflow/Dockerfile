FROM apache/airflow:3.3.2

# The image already has pandas and psycopg. Pinning apache-airflow stops pip from upgrading it.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" duckdb==2.0.0

# dbt gets its own virtualenv, so its dependencies can't clash with Airflow's.
USER root
RUN python -m venv /opt/dbt-venv \
    && PIP_USER=false /opt/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-postgres==1.11.0
USER airflow
