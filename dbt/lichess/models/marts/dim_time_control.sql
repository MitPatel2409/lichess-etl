-- One row per time-control class, shortest first. The classes are fixed rules, so they are listed here, not read from data.
select time_control, sort_order
from (
    values
        ('ultrabullet', 1),
        ('bullet', 2),
        ('blitz', 3),
        ('rapid', 4),
        ('classical', 5),
        ('correspondence', 6)
) as classes (time_control, sort_order)
