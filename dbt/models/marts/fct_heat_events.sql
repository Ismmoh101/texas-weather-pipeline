-- Heat events: runs of 1+ consecutive days with a high of 100°F or more.
-- Classic "gaps and islands": consecutive dates share the same
-- (date - row_number) value, which becomes the group key.

with hot_days as (
    select
        city_id,
        weather_date,
        max_temp_f,
        row_number() over (partition by city_id order by weather_date) as rn
    from {{ ref('fct_daily_weather') }}
    where max_temp_f >= 100
),

islands as (
    select
        *,
        {{ dbt.dateadd('day', '-1 * rn', 'weather_date') }} as island_key
    from hot_days
)

select
    city_id || '_' || cast(min(weather_date) as varchar)    as heat_event_key,
    city_id,
    min(weather_date)                                       as event_start,
    max(weather_date)                                       as event_end,
    count(*)                                                as duration_days,
    max(max_temp_f)                                         as peak_temp_f
from islands
group by city_id, island_key
