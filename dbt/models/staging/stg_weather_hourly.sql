with source as (
    select * from {{ source('raw', 'weather_hourly') }}
)

select
    city_id,
    observed_at                                         as observed_at_utc,
    {{ to_local_time('observed_at') }}                  as observed_at_local,
    temperature_2m                                      as temp_c,
    round(temperature_2m * 9.0 / 5 + 32, 1)             as temp_f,
    round(apparent_temperature * 9.0 / 5 + 32, 1)       as feels_like_f,
    relative_humidity_2m                                as humidity_pct,
    precipitation                                       as precip_mm,
    round(precipitation / 25.4, 3)                      as precip_in,
    round(wind_speed_10m * 0.621371, 1)                 as wind_mph,
    weather_code,
    ingested_at
from source
