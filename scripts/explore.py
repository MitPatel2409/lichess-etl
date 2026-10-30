"""Print the schema, row count and 5 sample rows of January 2013, and save them to docs/schema.md."""

from pathlib import Path

import duckdb

URL = "hf://datasets/Lichess/standard-chess-games/data/year=2013/month=01/*.parquet"

# The columns the pipeline will use. movetext is never read.
COLUMNS = "UTCDate, UTCTime, White, Black, WhiteElo, BlackElo, Result, TimeControl, ECO, Opening, Termination"

# DESCRIBE and COUNT(*) only read Parquet metadata.
schema = duckdb.sql(f"DESCRIBE SELECT * FROM '{URL}'")
row_count = duckdb.sql(f"SELECT COUNT(*) FROM '{URL}'").fetchall()[0][0]
sample = duckdb.sql(f"SELECT {COLUMNS} FROM '{URL}' LIMIT 5")

report = f"""# January 2013 schema

Source: `{URL}`

## Columns

```
{schema}
```

## Row count

{row_count:,} games

## Sample (5 rows, the columns the pipeline uses)

```
{sample}
```
"""

print(report)
out = Path(__file__).resolve().parent.parent / "docs" / "schema.md"
out.parent.mkdir(exist_ok=True)
out.write_text(report, encoding="utf-8")
