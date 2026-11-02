-- Every game ends in exactly one result, so this must return no rows.
select *
from {{ ref('fct_game_outcomes') }}
where white_wins + black_wins + draws <> games
