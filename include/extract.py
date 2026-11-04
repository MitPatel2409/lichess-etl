"""DuckDB queries that turn one month of Lichess Parquet into small aggregate DataFrames.

DuckDB reads the Parquet straight from Hugging Face's CDN, fetching only the columns each query uses,
so the raw games never reach the disk. Each query returns a few thousand to a few million summary rows.
"""

import json
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pandas as pd

# --- Where the files are ---

HUGGING_FACE = "https://huggingface.co"
DATASET = "datasets/Lichess/standard-chess-games"
LIST_FILES_URL = HUGGING_FACE + "/api/" + DATASET + "/tree/main/data/year={year}/month={month:02d}"
FILE_URL = HUGGING_FACE + "/" + DATASET + "/resolve/main/{path}"

# --- How DuckDB runs ---

DUCKDB_CONFIG = {
    "memory_limit": "2GB",
    # The CDN links hold a literal * (in filename*=UTF-8...), which DuckDB would otherwise read as a glob.
    "allow_asterisks_in_http_paths": True,
    # The work is mostly waiting on small HTTP reads, so more threads keep more requests in flight
    # (one pass over 2016-01: 43 s with the default 12, 18 s with 32, no faster with 64).
    "threads": 32,
    # Retry a failed read after 1, 2, 4 ... 128 s, so a network drop of up to ~4 minutes doesn't fail
    # the run (the default gives up after ~2 s; a drop 40 minutes into 2024-01 lost the whole run).
    "http_retries": 8,
    "http_retry_wait_ms": 1000,
    "http_retry_backoff": 2,
}

# --- Rules shared by every query, as SQL macros (named expressions) ---

MACROS_SQL = """
-- The games every table counts. All three queries read from here, so they always agree.
-- Dropped: games with a missing rating (NULL), and games with no result ('*': abandoned or unterminated,
-- nearly all after 0-1 moves; first seen in 2025, where they were 0.18% of the month).
CREATE MACRO counted_games(url) AS TABLE
    SELECT *
    FROM read_parquet(url)
    WHERE WhiteElo IS NOT NULL
      AND BlackElo IS NOT NULL
      AND Result IN ('1-0', '0-1', '1/2-1/2');

-- Estimated duration = base seconds + 40 x increment. TRY_CAST gives NULL for '-'.
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

-- Bands are 200 points wide and named by their lower edge (1400 = 1400-1599).
CREATE MACRO elo_band(elo) AS elo // 200 * 200;
"""

# --- The three queries ---
# The month comes from the params, not UTCDate: games started on Dec 31 are filed under January.

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
FROM counted_games($url)
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
    FROM counted_games($url)
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

# weekday is ISO: 1 = Monday ... 7 = Sunday. Hours are UTC, 0-23.
ACTIVITY_BY_HOUR_SQL = """
SELECT
    make_date($year, $month, 1) AS month,
    isodow(UTCDate) AS weekday,
    hour(CAST(UTCTime AS TIME)) AS hour,
    time_control_class(TimeControl) AS time_control_class,
    COUNT(*) AS games
FROM counted_games($url)
GROUP BY ALL
ORDER BY ALL
"""


# --- Finding the files ---


def month_urls(year: int, month: int) -> list[str]:
    """CDN links to a month's Parquet files.

    Reading through hf:// costs one Hugging Face lookup per chunk DuckDB fetches, and a recent month has
    about 100,000 chunks, far over the limit of 3,000 lookups per 5 minutes. Looking up each file once
    costs one lookup per file (about 420). The links expire after an hour, so each query gets fresh ones.
    """
    with urllib.request.urlopen(LIST_FILES_URL.format(year=year, month=month)) as response:
        paths = [f["path"] for f in json.load(response) if f["path"].endswith(".parquet")]
    with ThreadPoolExecutor(max_workers=8) as pool:
        return list(pool.map(_cdn_link, paths))


def _cdn_link(path: str) -> str:
    # Hugging Face answers a file request with a redirect to its CDN. A HEAD request follows it without
    # downloading anything, and .url is where it landed.
    request = urllib.request.Request(FILE_URL.format(path=path), method="HEAD")
    with urllib.request.urlopen(request) as response:
        return response.url


# --- Running the queries ---


def _run(query: str, year: int, month: int, url: str | list[str] | None) -> pd.DataFrame:
    """Run one query against a month's Parquet. `url` replaces the Hugging Face files; tests point it at a local file."""
    con = duckdb.connect(config=DUCKDB_CONFIG)
    try:
        con.execute(MACROS_SQL)
        params = {"year": year, "month": month, "url": url or month_urls(year, month)}
        return con.execute(query, params).df()
    finally:
        con.close()


def extract_game_aggregates(year: int, month: int, url: str | list[str] | None = None) -> pd.DataFrame:
    """Game counts and results for one month, grouped by time control, both Elo bands, opening and termination."""
    return _run(GAME_AGGREGATES_SQL, year, month, url)


def extract_player_month(year: int, month: int, url: str | list[str] | None = None) -> pd.DataFrame:
    """One row per player and time control for one month: games, wins, average and last rating, Elo band."""
    return _run(PLAYER_MONTH_SQL, year, month, url)


def extract_activity_by_hour(year: int, month: int, url: str | list[str] | None = None) -> pd.DataFrame:
    """Games played in one month, by weekday, hour (UTC) and time control."""
    return _run(ACTIVITY_BY_HOUR_SQL, year, month, url)


def extract_month(year: int, month: int) -> dict[str, pd.DataFrame]:
    """All three tables for one month, keyed by their raw table name. Used by the DAG and scripts/run_month.py."""
    return {
        "game_aggregates": extract_game_aggregates(year, month),
        "player_month": extract_player_month(year, month),
        "activity_by_hour": extract_activity_by_hour(year, month),
    }
