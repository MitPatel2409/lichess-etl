-- Same grain as raw.player_month (month x player x time control). Renames columns to match the other models.
select
    month,
    player as player_id,
    time_control_class as time_control,
    elo_band,
    games,
    wins,
    avg_rating,
    last_rating
from {{ source('raw', 'player_month') }}
