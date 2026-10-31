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

from include.extract import extract_game_aggregates  # noqa: E402
from include.load import load_game_aggregates  # noqa: E402

parser = argparse.ArgumentParser()
parser.add_argument("year", type=int)
parser.add_argument("month", type=int)
args = parser.parse_args()

load_dotenv()
df = extract_game_aggregates(args.year, args.month)

with psycopg.connect(
    host="localhost",
    port=5432,
    dbname=os.environ["POSTGRES_DB"],
    user=os.environ["POSTGRES_USER"],
    password=os.environ["POSTGRES_PASSWORD"],
) as conn:
    load_game_aggregates(conn, args.year, args.month, df)

print(f"Loaded {len(df):,} rows for {args.year}-{args.month:02d}")
