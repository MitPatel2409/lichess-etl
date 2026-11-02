-- One row per loaded month. Power BI uses this as its date table.
select distinct
    month as month_date,
    extract(year from month)::int as year,
    extract(month from month)::int as month_number
from {{ ref('stg_game_aggregates') }}
