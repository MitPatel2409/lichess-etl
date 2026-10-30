"""DuckDB queries that turn one month of Lichess Parquet into small aggregate DataFrames."""

import duckdb
import pandas as pd

URL = "hf://datasets/Lichess/standard-chess-games/data/year={year}/month={month:02d}/*.parquet"

# The month comes from the folder, not UTCDate: games started on Dec 31 are filed under January.
# Ratings are SMALLINT, so missing ratings are NULL. Bands are 200 points wide (1400 = 1400-1599).
GAME_AGGREGATES_SQL = """
SELECT
    make_date($year, $month, 1) AS month,
    CASE
        WHEN TimeControl = '-' THEN 'correspondence'
        WHEN est_seconds < 29 THEN 'ultrabullet'
        WHEN est_seconds < 179 THEN 'bullet'
        WHEN est_seconds < 479 THEN 'blitz'
        WHEN est_seconds < 1499 THEN 'rapid'
        ELSE 'classical'
    END AS time_control_class,
    WhiteElo // 200 * 200 AS white_elo_band,
    BlackElo // 200 * 200 AS black_elo_band,
    ECO AS eco,
    Opening AS opening,
    Termination AS termination,
    COUNT(*) AS games,
    COUNT(*) FILTER (WHERE Result = '1-0') AS white_wins,
    COUNT(*) FILTER (WHERE Result = '0-1') AS black_wins,
    COUNT(*) FILTER (WHERE Result = '1/2-1/2') AS draws
FROM (
    -- Estimated duration = base seconds + 40 x increment. TRY_CAST gives NULL for '-'.
    SELECT
        WhiteElo, BlackElo, Result, TimeControl, ECO, Opening, Termination,
        TRY_CAST(split_part(TimeControl, '+', 1) AS INTEGER)
            + 40 * TRY_CAST(split_part(TimeControl, '+', 2) AS INTEGER) AS est_seconds
    FROM read_parquet($url)
    WHERE WhiteElo IS NOT NULL AND BlackElo IS NOT NULL
)
GROUP BY ALL
ORDER BY ALL
"""


def extract_game_aggregates(year: int, month: int, url: str | None = None) -> pd.DataFrame:
    """Game counts and results for one month, grouped by time control, both Elo bands, opening and termination.

    `url` replaces the Hugging Face path; tests point it at a local Parquet file.
    """
    con = duckdb.connect(config={"memory_limit": "2GB"})
    try:
        url = url or URL.format(year=year, month=month)
        return con.execute(GAME_AGGREGATES_SQL, {"year": year, "month": month, "url": url}).df()
    finally:
        con.close()
