-- One row per month x player x time control. Count players with DISTINCTCOUNT(player_id):
-- one player can play several time controls, so per-class counts don't add up to the month.
-- last_rating is the rating going into the player's latest game of the month; elo_band comes from it.
select
    month,
    player_id,
    time_control,
    elo_band,
    games,
    wins,
    avg_rating,
    last_rating
from {{ ref('stg_player_month') }}
