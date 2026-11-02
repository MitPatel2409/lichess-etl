-- One row per 200-point Elo band seen for either colour. elo_band is the lower edge; sort the label by it.
with bands as (
    select white_elo_band as elo_band from {{ ref('stg_game_aggregates') }}
    union
    select black_elo_band from {{ ref('stg_game_aggregates') }}
)

select
    elo_band,
    elo_band || '-' || (elo_band + 199) as band_label
from bands
