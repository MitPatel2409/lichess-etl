-- One row per ECO code + opening name. opening_id is a hash of both, so Power BI can join on one column.
select distinct
    md5(eco || '|' || opening_name) as opening_id,
    eco,
    opening_name
from {{ ref('stg_game_aggregates') }}
