-- Same grain as raw.activity_by_hour (month x weekday x hour x time control). Renames columns to match the other models.
select
    month,
    weekday,
    hour,
    time_control_class as time_control,
    games
from {{ source('raw', 'activity_by_hour') }}
