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

        from include.extract import extract_activity_by_hour, extract_game_aggregates, extract_player_month
        from include.load import replace_month

        year, month = params["year"], params["month"]
        tables = {
            "game_aggregates": extract_game_aggregates(year, month),
            "player_month": extract_player_month(year, month),
            "activity_by_hour": extract_activity_by_hour(year, month),
        }
        with psycopg.connect(
            host="postgres",
            dbname=os.environ["POSTGRES_DB"],
            user=os.environ["POSTGRES_USER"],
            password=os.environ["POSTGRES_PASSWORD"],
        ) as conn:
            for table, df in tables.items():
                replace_month(conn, table, year, month, df)
                print(f"Loaded {len(df):,} rows into raw.{table} for {year}-{month:02d}")

    # Rebuilds every model and runs every test. A failing test fails the task.
    @task.bash(cwd="/opt/airflow/dbt/lichess")
    def dbt_build():
        return "/opt/dbt-venv/bin/dbt build"

    load_month() >> dbt_build()


lichess_monthly()
