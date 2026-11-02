"""Load one month of Lichess games into Postgres. Trigger it with a year and month; it never runs on a schedule."""

from airflow.sdk import Param, dag, task


@dag(
    schedule=None,
    params={
        "year": Param(2013, type="integer", minimum=2013),
        "month": Param(1, type="integer", minimum=1, maximum=12),
    },
)
def lichess_monthly():
    @task
    def load_month(params=None):
        # Imported here, not at the top: Airflow re-parses this file often, and DuckDB and pandas are slow to import.
        import os

        import psycopg

        from include.extract import extract_game_aggregates
        from include.load import load_game_aggregates

        year, month = params["year"], params["month"]
        df = extract_game_aggregates(year, month)
        with psycopg.connect(
            host="postgres",
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            load_game_aggregates(conn, year, month, df)
        print(f"Loaded {len(df):,} rows for {year}-{month:02d}")

    load_month()


lichess_monthly()
