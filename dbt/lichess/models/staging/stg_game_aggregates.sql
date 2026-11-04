-- Same grain as raw.game_aggregates. Renames columns, fills missing text, and groups rare terminations
-- (rules infraction, insufficient material, ...) into 'Other'.
select
    month,
    time_control_class as time_control,
    white_elo_band,
    black_elo_band,
    coalesce(eco, '?') as eco,
    coalesce(opening, 'Unknown') as opening_name,
    case termination
        when 'Normal' then 'Normal'
        when 'Time forfeit' then 'Time forfeit'
        when 'Abandoned' then 'Abandoned'
        else 'Other'
    end as termination,
    games,
    white_wins,
    black_wins,
    draws
from {{ source('raw', 'game_aggregates') }}
