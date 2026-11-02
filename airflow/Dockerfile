FROM apache/airflow:3.3.2

# The image already has pandas and psycopg. Pinning apache-airflow stops pip from upgrading it.
RUN pip install --no-cache-dir "apache-airflow==3.3.2" duckdb==2.0.0
