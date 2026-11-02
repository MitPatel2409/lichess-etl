"""Load one month into a raw table so that rerunning a month replaces it instead of duplicating it."""

from datetime import date

import pandas as pd
import psycopg
from psycopg import sql


def replace_month(conn: psycopg.Connection, table: str, year: int, month: int, df: pd.DataFrame) -> None:
    """Delete the month's rows from raw.<table>, then insert the new ones, in one transaction."""
    # NaN/NA become None (SQL NULL), and numpy numbers become plain Python values.
    rows = df.astype(object).where(df.notna(), None).itertuples(index=False)
    # Table and column names are quoted as identifiers, never pasted into the SQL as text.
    target = sql.Identifier("raw", table)
    columns = sql.SQL(", ").join(map(sql.Identifier, df.columns))

    # If anything fails, the transaction rolls back and the month's old rows stay as they were.
    with conn.transaction():
        conn.execute(sql.SQL("DELETE FROM {} WHERE month = %s").format(target), (date(year, month, 1),))
        with conn.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(target, columns)) as copy:
            for row in rows:
                copy.write_row(row)
