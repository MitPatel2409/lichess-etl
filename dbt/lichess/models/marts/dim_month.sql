-- One row per loaded month. Every fact joins to it, and Power BI's month slicer reads from it.
select distinct
    month as month_date,
    extract(year from month)::int as year,
    extract(month from month)::int as month_number
from {{ ref('stg_game_aggregates') }}
