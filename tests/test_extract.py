import duckdb
import pandas as pd
import pytest

from include.extract import extract_activity_by_hour, extract_game_aggregates, extract_player_month

# Ten games, each one testing a rule. There is no movetext column, so the tests
# fail if an extraction ever starts reading it.
ROWS = [
    # White, Black, WhiteElo, BlackElo, Result, UTCDate, UTCTime, TimeControl, ECO, Opening, Termination
    ("alice", "bob", 1650, 1420, "1-0", "2013-01-06", "10:00:00+00", "180+0", "C00", "French Defense", "Normal"),  # 180 s: lowest blitz
    ("carol", "bob", 1600, 1599, "0-1", "2013-01-03", "10:00:00+00", "300+3", "C00", "French Defense", "Normal"),  # band edges: 1600 -> 1600, 1599 -> 1400
    ("alice", "dave", 1799, 1400, "1/2-1/2", "2013-01-01", "09:00:00+00", "420+0", "C00", "French Defense", "Normal"),  # same group as the two above; alice's earlier game
    ("erin", "dave", 1500, 1500, "0-1", "2013-01-02", "12:00:00+00", "15+0", "B01", "Scandinavian Defense", "Time forfeit"),  # ultrabullet
    ("erin", "dave", 1500, 1500, "1-0", "2013-01-02", "12:10:00+00", "120+1", "B01", "Scandinavian Defense", "Normal"),  # 160 s: bullet
    ("erin", "dave", 1500, 1500, "1-0", "2013-01-02", "12:20:00+00", "480+0", "B01", "Scandinavian Defense", "Normal"),  # 480 s: lowest rapid
    ("erin", "dave", 1500, 1500, "1-0", "2013-01-02", "12:30:00+00", "1500+0", "B01", "Scandinavian Defense", "Normal"),  # 1500 s: lowest classical
    ("erin", "dave", 1500, 1500, "1-0", "2013-01-02", "12:40:00+00", "-", "B01", "Scandinavian Defense", "Normal"),  # no clock: correspondence
    ("frank", "erin", None, 1500, "1-0", "2013-01-04", "08:00:00+00", "180+0", "C00", "French Defense", "Normal"),  # missing White rating: dropped
    ("erin", "gina", 1500, None, "0-1", "2013-01-04", "08:10:00+00", "180+0", "C00", "French Defense", "Normal"),  # missing Black rating: dropped
]


@pytest.fixture(scope="module")
def games_parquet(tmp_path_factory):
    path = (tmp_path_factory.mktemp("fixture") / "games.parquet").as_posix()
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE games (
            White VARCHAR, Black VARCHAR, WhiteElo SMALLINT, BlackElo SMALLINT, Result VARCHAR,
            UTCDate DATE, UTCTime TIMETZ, TimeControl VARCHAR, ECO VARCHAR, Opening VARCHAR, Termination VARCHAR
        )
    """)
    con.executemany("INSERT INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", ROWS)
    con.execute(f"COPY games TO '{path}' (FORMAT parquet)")
    con.close()
    return path


@pytest.fixture(scope="module")
def game_aggregates(games_parquet):
    return extract_game_aggregates(2013, 1, url=games_parquet)


@pytest.fixture(scope="module")
def player_month(games_parquet):
    return extract_player_month(2013, 1, url=games_parquet)


@pytest.fixture(scope="module")
def activity_by_hour(games_parquet):
    return extract_activity_by_hour(2013, 1, url=games_parquet)


# --- game_aggregates ---


def test_drops_games_with_missing_ratings(game_aggregates):
    assert game_aggregates["games"].sum() == 8


def test_month_comes_from_the_params(game_aggregates):
    assert (game_aggregates["month"] == pd.Timestamp("2013-01-01")).all()


def test_time_control_classes(game_aggregates):
    games_by_class = game_aggregates.groupby("time_control_class")["games"].sum().to_dict()
    assert games_by_class == {
        "ultrabullet": 1,
        "bullet": 1,
        "blitz": 3,
        "rapid": 1,
        "classical": 1,
        "correspondence": 1,
    }


def test_games_in_the_same_group_are_counted_together(game_aggregates):
    group = game_aggregates[(game_aggregates["eco"] == "C00") & (game_aggregates["time_control_class"] == "blitz")]
    assert len(group) == 1
    row = group.iloc[0]
    assert (row["white_elo_band"], row["black_elo_band"]) == (1600, 1400)
    assert (row["games"], row["white_wins"], row["black_wins"], row["draws"]) == (3, 1, 1, 1)


def test_results_add_up_to_games_on_every_row(game_aggregates):
    assert (game_aggregates["white_wins"] + game_aggregates["black_wins"] + game_aggregates["draws"] == game_aggregates["games"]).all()


# --- player_month ---


def test_each_game_counts_once_for_each_player(player_month):
    assert player_month["games"].sum() == 2 * 8


def test_players_from_dropped_games_are_left_out(player_month):
    # frank and gina only played games with a missing rating.
    assert set(player_month["player"]) == {"alice", "bob", "carol", "dave", "erin"}


def test_one_row_per_player_and_time_control(player_month):
    assert not player_month.duplicated(["player", "time_control_class"]).any()
    assert len(player_month[player_month["player"] == "dave"]) == 6


def test_games_wins_and_ratings_for_one_player(player_month):
    row = player_month[(player_month["player"] == "alice") & (player_month["time_control_class"] == "blitz")].iloc[0]
    assert (row["games"], row["wins"]) == (2, 1)
    assert row["avg_rating"] == (1650 + 1799) / 2


def test_last_rating_comes_from_the_latest_game_not_the_last_row(player_month):
    # alice played at 1799 on Jan 1 and at 1650 on Jan 6; the Jan 6 game comes first in the file.
    row = player_month[(player_month["player"] == "alice") & (player_month["time_control_class"] == "blitz")].iloc[0]
    assert (row["last_rating"], row["elo_band"]) == (1650, 1600)


# --- activity_by_hour ---


def test_games_by_hour_add_up_to_the_month(activity_by_hour, game_aggregates):
    assert activity_by_hour["games"].sum() == game_aggregates["games"].sum() == 8


def test_weekday_is_iso_and_hour_is_utc(activity_by_hour):
    # 2013-01-01 was a Tuesday (2), Jan 2 a Wednesday (3), Jan 3 a Thursday (4), Jan 6 a Sunday (7, not 0).
    games_by_slot = activity_by_hour.groupby(["weekday", "hour"])["games"].sum().to_dict()
    assert games_by_slot == {(2, 9): 1, (3, 12): 5, (4, 10): 1, (7, 10): 1}


def test_one_row_per_time_control_within_an_hour(activity_by_hour):
    wednesday_noon = activity_by_hour[(activity_by_hour["weekday"] == 3) & (activity_by_hour["hour"] == 12)]
    assert len(wednesday_noon) == 5
    assert (wednesday_noon["games"] == 1).all()
