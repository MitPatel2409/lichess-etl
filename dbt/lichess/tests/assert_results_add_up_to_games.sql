-- Every counted game ends in exactly one result (games without one, '*', are dropped at extraction),
-- so this must return no rows.
select *
from {{ ref('fct_game_outcomes') }}
where white_wins + black_wins + draws <> games
