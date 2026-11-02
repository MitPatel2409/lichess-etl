-- Runs once, when Postgres starts with an empty data volume.

-- Airflow's own metadata lives in a separate database on the same server.
CREATE DATABASE airflow;

CREATE SCHEMA raw;

-- One row per month x time-control class x White band x Black band x ECO x opening x termination.
-- Bands are the lower edge of a 200-point bucket (1400 = 1400-1599).
CREATE TABLE raw.game_aggregates (
    month              DATE     NOT NULL,
    time_control_class TEXT     NOT NULL,
    white_elo_band     SMALLINT NOT NULL,
    black_elo_band     SMALLINT NOT NULL,
    eco                TEXT,
    opening            TEXT,
    termination        TEXT,
    games              INTEGER  NOT NULL,
    white_wins         INTEGER  NOT NULL,
    black_wins         INTEGER  NOT NULL,
    draws              INTEGER  NOT NULL
);

-- One row per month x player x time-control class.
-- last_rating is the rating going into the player's latest game of the month; elo_band comes from it.
CREATE TABLE raw.player_month (
    month              DATE         NOT NULL,
    player             TEXT,
    time_control_class TEXT         NOT NULL,
    games              INTEGER      NOT NULL,
    wins               INTEGER      NOT NULL,
    avg_rating         NUMERIC(5,1) NOT NULL,
    last_rating        SMALLINT     NOT NULL,
    elo_band           SMALLINT     NOT NULL
);
