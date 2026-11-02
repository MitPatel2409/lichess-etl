"""DuckDB queries that turn one month of Lichess Parquet into small aggregate DataFrames."""

import duckdb
import pandas as pd

URL = "hf://datasets/Lichess/standard-chess-games/data/year={year}/month={month:02d}/*.parquet"

# Rules shared by every query, as SQL macros (named expressions).
# Estimated duration = base seconds + 40 x increment. TRY_CAST gives NULL for '-'.
# Bands are 200 points wide and named by their lower edge (1400 = 1400-1599).
MACROS_SQL = """
CREATE MACRO est_seconds(tc) AS
    TRY_CAST(split_part(tc, '+', 1) AS INTEGER) + 40 * TRY_CAST(split_part(tc, '+', 2) AS INTEGER);

CREATE MACRO time_control_class(tc) AS
    CASE
        WHEN tc = '-' THEN 'correspondence'
        WHEN est_seconds(tc) < 29 THEN 'ultrabullet'
        WHEN est_seconds(tc) < 179 THEN 'bullet'
        WHEN est_seconds(tc) < 479 THEN 'blitz'
        WHEN est_seconds(tc) < 1499 THEN 'rapid'
        ELSE 'classical'
    END;

CREATE MACRO elo_band(elo) AS elo // 200 * 200;
"""

# Every query: the month comes from the folder, not UTCDate (games started on Dec 31 are
# filed under January), and games with a missing rating (NULL) are dropped.
GAME_AGGREGATES_SQL = """
SELECT
    make_date($year, $month, 1) AS month,
    time_control_class(TimeControl) AS time_control_class,
    elo_band(WhiteElo) AS white_elo_band,
    elo_band(BlackElo) AS black_elo_band,
    ECO AS eco,
    Opening AS opening,
    Termination AS termination,
    COUNT(*) AS games,
    COUNT(*) FILTER (WHERE Result = '1-0') AS white_wins,
    COUNT(*) FILTER (WHERE Result = '0-1') AS black_wins,
    COUNT(*) FILTER (WHERE Result = '1/2-1/2') AS draws
FROM read_parquet($url)
WHERE WhiteElo IS NOT NULL AND BlackElo IS NOT NULL
GROUP BY ALL
ORDER BY ALL
"""

# Each game becomes two rows, one per player. UNNEST does that in a single scan of the files.
# last_rating is the rating going into the player's latest game of the month.
PLAYER_MONTH_SQL = """
WITH player_games AS (
    SELECT
        time_control_class(TimeControl) AS time_control_class,
        UTCDate + CAST(UTCTime AS TIME) AS played_at,
        UNNEST([
            {'player': White, 'rating': WhiteElo, 'won': Result = '1-0'},
            {'player': Black, 'rating': BlackElo, 'won': Result = '0-1'}
        ], recursive := true)
    FROM read_parquet($url)
    WHERE WhiteElo IS NOT NULL AND BlackElo IS NOT NULL
)
SELECT
    make_date($year, $month, 1) AS month,
    player,
    time_control_class,
    COUNT(*) AS games,
    COUNT(*) FILTER (WHERE won) AS wins,
    ROUND(AVG(rating), 1) AS avg_rating,
    arg_max(rating, played_at) AS last_rating,
    elo_band(arg_max(rating, played_at)) AS elo_band
FROM player_games
GROUP BY player, time_control_class
ORDER BY ALL
"""


def _run(query: str, year: int, month: int, url: str | None) -> pd.DataFrame:
    """Run one query against a month's Parquet. `url` replaces the Hugging Face path; tests point it at a local file."""
    con = duckdb.connect(config={"memory_limit": "2GB"})
    try:
        con.execute(MACROS_SQL)
        url = url or URL.format(year=year, month=month)
        return con.execute(query, {"year": year, "month": month, "url": url}).df()
    finally:
        con.close()


def extract_game_aggregates(year: int, month: int, url: str | None = None) -> pd.DataFrame:
    """Game counts and results for one month, grouped by time control, both Elo bands, opening and termination."""
    return _run(GAME_AGGREGATES_SQL, year, month, url)


def extract_player_month(year: int, month: int, url: str | None = None) -> pd.DataFrame:
    """One row per player and time control for one month: games, wins, average and last rating, Elo band."""
    return _run(PLAYER_MONTH_SQL, year, month, url)
