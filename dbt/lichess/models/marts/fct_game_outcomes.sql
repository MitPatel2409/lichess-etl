-- One row per month x time control x White band x Black band x opening x termination.
-- Summed again because folding rare terminations into 'Other' can merge staging rows.
select
    month,
    time_control,
    white_elo_band,
    black_elo_band,
    md5(eco || '|' || opening_name) as opening_id,
    termination,
    sum(games) as games,
    sum(white_wins) as white_wins,
    sum(black_wins) as black_wins,
    sum(draws) as draws
from {{ ref('stg_game_aggregates') }}
group by 1, 2, 3, 4, 5, 6
