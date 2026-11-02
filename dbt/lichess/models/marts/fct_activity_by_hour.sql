-- One row per month x weekday x hour x time control. weekday is ISO (1 = Monday ... 7 = Sunday); hour is UTC.
select
    month,
    weekday,
    hour,
    time_control,
    games
from {{ ref('stg_activity_by_hour') }}
