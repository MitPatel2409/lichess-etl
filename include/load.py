"""Load one month of aggregates into Postgres so that rerunning a month replaces it instead of duplicating it."""

from datetime import date

import pandas as pd
import psycopg


def load_game_aggregates(conn: psycopg.Connection, year: int, month: int, df: pd.DataFrame) -> None:
    """Delete the month's rows, then insert the new ones, in one transaction."""
    # NaN/NA become None (SQL NULL), and numpy numbers become plain Python ints.
    rows = df.astype(object).where(df.notna(), None).itertuples(index=False)
    columns = ", ".join(df.columns)

    # If anything fails, the transaction rolls back and the month's old rows stay as they were.
    with conn.transaction():
        conn.execute("DELETE FROM raw.game_aggregates WHERE month = %s", (date(year, month, 1),))
        with conn.cursor().copy(f"COPY raw.game_aggregates ({columns}) FROM STDIN") as copy:
            for row in rows:
                copy.write_row(row)
