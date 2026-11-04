# Lichess ETL

An on-demand data pipeline that turns one month of Lichess's public game archive into a small analytics warehouse. You pick a year and month and trigger an Airflow DAG. DuckDB reads that month's Parquet files straight from Hugging Face, aggregates them as it reads (so billions of raw games never reach the database), and loads compact tables into PostgreSQL in a rerun-safe transaction. dbt then models those tables into a tested star schema, and a Power BI dashboard shows the month: game volume, win rates, Elo bands, openings, play times and how games end.

## Performance

Each row is one trigger of `lichess_monthly`, on a Windows 11 laptop (16 GB RAM, 12 threads) with Docker given 8 GB:

| Month | Games in the files | Parquet on Hugging Face | `load_month` | `dbt_build` | Whole run | Peak memory (task container) |
| --- | --- | --- | --- | --- | --- | --- |
| January 2016 (before the 32-thread tuning below) | 4,770,357 | 8 files, 1.87 GB | 17 min 26 s | 9 s | 17 min 35 s | 1.6 GB |
| **September 2025** (latest) | **87,049,890** | **62 files, 62.5 GB** | **32 min 38 s** | **17 s** | **33 min** | **5.9 GB** |

- **Reading, not downloading.** For September 2025, DuckDB pulled about 11.5 GB over the network (only the columns it needs, across three passes) out of 62.5 GB, and saved none of it to disk.
- **Output:** 785,021 game aggregates, 2,863,871 player-months (1,874,784 unique players) and 1,008 activity rows. The whole database (four months) is 1.35 GB.
- **Memory:** the peak of 5.9 GB of the 7.6 GB container limit comes from the `player_month` pass (about 1.9 million players). Airflow itself uses about 0.7 GB; DuckDB is capped at 2 GB. Postgres peaked at 1.3 GB.
- **Bottleneck: request latency, not bandwidth or CPU.** The files are cut into 1,000-row chunks (87,110 for September 2025), so DuckDB makes hundreds of thousands of small requests, each mostly waiting on the round trip. The task averaged 148% CPU out of 1,200%, and the network ran at 3-7 MB/s on a connection that does about 14 MB/s. Raising DuckDB to 32 threads made one pass 2.4x faster (43 s to 18 s on January 2016); 64 threads were no faster.

What changed after trying recent months:

- **Hugging Face rate limit (fixed).** Reading through `hf://` paths costs one Hugging Face lookup per chunk, and Hugging Face allows 3,000 lookups per 5 minutes, so January 2024 (about 99,000 chunks) failed with HTTP 429 after 4 minutes. `month_urls` in `include/extract.py` now looks up each file's CDN link once (62 lookups for September 2025), and DuckDB reads the chunks from the CDN, which has no such limit.
- **Network drops (fixed).** A drop 40 minutes into January 2024 failed the whole run, because a DuckDB query can't resume. DuckDB now retries a failed read for up to about 4 minutes. A longer outage still means rerunning the month, which is safe because loads are rerun-safe.
- **Games with no result (fixed).** The first September 2025 load failed the dbt test that White wins + Black wins + draws = games: 158,477 games (0.18%) had the result `*`. They were abandoned or unterminated, and about 85% had no moves or only White's first move. Months from 2013 and 2016 have none. The rule for which games count now lives in one SQL macro, `counted_games`, that all three queries read from, and it leaves these games out. September 2025 was then rerun: 86,891,413 games counted, all 36 dbt tests pass.
- **The cost of not storing raw data.** Because nothing raw is kept, a rule change like the one above means reading the month from Hugging Face again (33 minutes for September 2025). Saving just the needed columns as local Parquet (about 4 GB for that month) would make reruns take minutes, at the cost of disk space.
