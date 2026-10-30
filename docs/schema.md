# January 2013 schema

Source: `hf://datasets/Lichess/standard-chess-games/data/year=2013/month=01/*.parquet`

## Columns

```
┌─────────────────┬─────────────────────┬─────────┬─────────┬─────────┬─────────┐
│   column_name   │     column_type     │  null   │   key   │ default │  extra  │
│     varchar     │       varchar       │ varchar │ varchar │ varchar │ varchar │
├─────────────────┼─────────────────────┼─────────┼─────────┼─────────┼─────────┤
│ Event           │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ Site            │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ White           │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ Black           │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ Result          │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ WhiteTitle      │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ BlackTitle      │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ WhiteElo        │ SMALLINT            │ YES     │ NULL    │ NULL    │ NULL    │
│ BlackElo        │ SMALLINT            │ YES     │ NULL    │ NULL    │ NULL    │
│ WhiteRatingDiff │ SMALLINT            │ YES     │ NULL    │ NULL    │ NULL    │
│ BlackRatingDiff │ SMALLINT            │ YES     │ NULL    │ NULL    │ NULL    │
│ UTCDate         │ DATE                │ YES     │ NULL    │ NULL    │ NULL    │
│ UTCTime         │ TIME WITH TIME ZONE │ YES     │ NULL    │ NULL    │ NULL    │
│ ECO             │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ Opening         │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ Termination     │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ TimeControl     │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ movetext        │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ month           │ VARCHAR             │ YES     │ NULL    │ NULL    │ NULL    │
│ year            │ BIGINT              │ YES     │ NULL    │ NULL    │ NULL    │
└─────────────────┴─────────────────────┴─────────┴─────────┴─────────┴─────────┘
  20 rows                                                             6 columns

```

## Row count

121,332 games

## Sample (5 rows, the columns the pipeline uses)

```
┌────────────┬─────────────────────┬──────────────────┬───────────────────┬──────────┬──────────┬─────────┬─────────────┬─────────┬─────────────────────────────────────────────┬─────────────┐
│  UTCDate   │       UTCTime       │      White       │       Black       │ WhiteElo │ BlackElo │ Result  │ TimeControl │   ECO   │                   Opening                   │ Termination │
│    date    │ time with time zone │     varchar      │      varchar      │  int16   │  int16   │ varchar │   varchar   │ varchar │                   varchar                   │   varchar   │
├────────────┼─────────────────────┼──────────────────┼───────────────────┼──────────┼──────────┼─────────┼─────────────┼─────────┼─────────────────────────────────────────────┼─────────────┤
│ 2012-12-31 │ 23:01:03+00         │ BFG9k            │ mamalak           │     1639 │     1403 │ 1-0     │ 600+8       │ C00     │ French Defense: Normal Variation            │ Normal      │
│ 2012-12-31 │ 23:04:12+00         │ Desmond_Wilson   │ savinka59         │     1654 │     1919 │ 1-0     │ 480+2       │ D04     │ Queen's Pawn Game: Colle System, Anti-Colle │ Normal      │
│ 2012-12-31 │ 23:03:15+00         │ Kozakmamay007    │ VanillaShamanilla │     1643 │     1747 │ 1-0     │ 420+17      │ C50     │ Four Knights Game: Italian Variation        │ Normal      │
│ 2012-12-31 │ 23:04:57+00         │ Naitero_Nagasaki │ 800               │     1824 │     1973 │ 0-1     │ 60+1        │ B12     │ Caro-Kann Defense: Goldman Variation        │ Normal      │
│ 2012-12-31 │ 23:02:37+00         │ nichiren1967     │ Naitero_Nagasaki  │     1765 │     1815 │ 0-1     │ 60+1        │ C00     │ French Defense: La Bourdonnais Variation    │ Normal      │
└────────────┴─────────────────────┴──────────────────┴───────────────────┴──────────┴──────────┴─────────┴─────────────┴─────────┴─────────────────────────────────────────────┴─────────────┘

```
