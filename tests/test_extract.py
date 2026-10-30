import duckdb
import pandas as pd
import pytest

from include.extract import extract_game_aggregates

# Ten games, each one testing a rule. There is no movetext column, so the test
# fails if the extraction ever starts reading it.
ROWS = [
    # WhiteElo, BlackElo, Result, TimeControl, ECO, Opening, Termination
    (1650, 1420, "1-0", "180+0", "C00", "French Defense", "Normal"),  # 180 s: lowest blitz
    (1600, 1599, "0-1", "300+3", "C00", "French Defense", "Normal"),  # band edges: 1600 -> 1600, 1599 -> 1400
    (1799, 1400, "1/2-1/2", "420+0", "C00", "French Defense", "Normal"),  # same group as the two above
    (1500, 1500, "0-1", "15+0", "B01", "Scandinavian Defense", "Time forfeit"),  # ultrabullet
    (1500, 1500, "1-0", "120+1", "B01", "Scandinavian Defense", "Normal"),  # 160 s: bullet
    (1500, 1500, "1-0", "480+0", "B01", "Scandinavian Defense", "Normal"),  # 480 s: lowest rapid
    (1500, 1500, "1-0", "1500+0", "B01", "Scandinavian Defense", "Normal"),  # 1500 s: lowest classical
    (1500, 1500, "1-0", "-", "B01", "Scandinavian Defense", "Normal"),  # no clock: correspondence
    (None, 1500, "1-0", "180+0", "C00", "French Defense", "Normal"),  # missing White rating: dropped
    (1500, None, "0-1", "180+0", "C00", "French Defense", "Normal"),  # missing Black rating: dropped
]


@pytest.fixture(scope="module")
def result(tmp_path_factory):
    path = (tmp_path_factory.mktemp("fixture") / "games.parquet").as_posix()
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE games (
            WhiteElo SMALLINT, BlackElo SMALLINT, Result VARCHAR, TimeControl VARCHAR,
            ECO VARCHAR, Opening VARCHAR, Termination VARCHAR
        )
    """)
    con.executemany("INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?)", ROWS)
    con.execute(f"COPY games TO '{path}' (FORMAT parquet)")
    con.close()
    return extract_game_aggregates(2013, 1, url=path)


def test_drops_games_with_missing_ratings(result):
    assert result["games"].sum() == 8


def test_month_comes_from_the_params(result):
    assert (result["month"] == pd.Timestamp("2013-01-01")).all()


def test_time_control_classes(result):
    games_by_class = result.groupby("time_control_class")["games"].sum().to_dict()
    assert games_by_class == {
        "ultrabullet": 1,
        "bullet": 1,
        "blitz": 3,
        "rapid": 1,
        "classical": 1,
        "correspondence": 1,
    }


def test_games_in_the_same_group_are_counted_together(result):
    group = result[(result["eco"] == "C00") & (result["time_control_class"] == "blitz")]
    assert len(group) == 1
    row = group.iloc[0]
    assert (row["white_elo_band"], row["black_elo_band"]) == (1600, 1400)
    assert (row["games"], row["white_wins"], row["black_wins"], row["draws"]) == (3, 1, 1, 1)


def test_results_add_up_to_games_on_every_row(result):
    assert (result["white_wins"] + result["black_wins"] + result["draws"] == result["games"]).all()
