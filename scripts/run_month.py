"""Extract one month from Hugging Face and load it into Postgres, without Airflow.

Usage: python scripts/run_month.py 2013 1
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# Make `include` importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from include.extract import extract_game_aggregates, extract_player_month  # noqa: E402
from include.load import replace_month  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("year", type=int)
parser.add_argument("month", type=int)
args = parser.parse_args()

load_dotenv()
tables = {
    "game_aggregates": extract_game_aggregates(args.year, args.month),
    "player_month": extract_player_month(args.year, args.month),
}

with psycopg.connect(
    host="localhost",
    port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
) as conn:
    for table, df in tables.items():
        replace_month(conn, table, args.year, args.month, df)
        print(f"Loaded {len(df):,} rows into raw.{table} for {args.year}-{args.month:02d}")
