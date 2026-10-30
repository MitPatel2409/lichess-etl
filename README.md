# Lichess ETL

An on-demand data pipeline that turns one month of Lichess's public game archive into a small analytics warehouse. You pick a year and month and trigger an Airflow DAG. DuckDB reads that month's Parquet files straight from Hugging Face, aggregates them as it reads (so billions of raw games never reach the database), and loads compact tables into PostgreSQL in a rerun-safe transaction. dbt then models those tables into a tested star schema, and a Power BI dashboard shows the month: game volume, win rates, Elo bands, openings, play times and how games end.
