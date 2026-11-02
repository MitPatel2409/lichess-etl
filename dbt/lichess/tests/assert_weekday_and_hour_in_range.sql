-- weekday is ISO (1-7) and hour is UTC (0-23). Returns any row outside those ranges.
select *
from {{ ref('fct_activity_by_hour') }}
where weekday not between 1 and 7
   or hour not between 0 and 23
