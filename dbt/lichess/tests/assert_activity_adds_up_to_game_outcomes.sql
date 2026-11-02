-- Both facts count the same games, so their totals must match for every month. Returns the months that don't.
with outcomes as (
    select month, sum(games) as games from {{ ref('fct_game_outcomes') }} group by month
),

activity as (
    select month, sum(games) as games from {{ ref('fct_activity_by_hour') }} group by month
)

select coalesce(o.month, a.month) as month, o.games as outcome_games, a.games as activity_games
from outcomes as o
full outer join activity as a on o.month = a.month
where o.games is distinct from a.games
