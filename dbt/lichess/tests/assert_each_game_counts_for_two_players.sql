-- Every game has a White and a Black player, so player games must be exactly twice the games. Returns the months where they aren't.
with outcomes as (
    select month, sum(games) as games from {{ ref('fct_game_outcomes') }} group by month
),

players as (
    select month, sum(games) as games from {{ ref('fct_player_month') }} group by month
)

select coalesce(o.month, p.month) as month, o.games as outcome_games, p.games as player_games
from outcomes as o
full outer join players as p on o.month = p.month
where 2 * o.games is distinct from p.games
