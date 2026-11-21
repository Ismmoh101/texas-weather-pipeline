{{
    config(
        materialized='incremental',
        unique_key='city_day_key',
        incremental_strategy='delete+insert'
    )
}}

with hourly as (
    select
        *,
        cast(observed_at_local as date) as weather_date
    from {{ ref('stg_weather_hourly') }}
    {% if is_incremental() %}
    -- Reprocess the last 3 days so late-arriving hours are picked up.
    where cast(observed_at_local as date) >= (
        select {{ dbt.dateadd('day', -3, 'max(weather_date)') }} from {{ this }}
    )
    {% endif %}
)

select
    city_id || '_' || cast(weather_date as varchar)     as city_day_key,
    city_id,
    weather_date,
    min(temp_f)                                         as min_temp_f,
    max(temp_f)                                         as max_temp_f,
    round(avg(temp_f), 1)                               as avg_temp_f,
    max(feels_like_f)                                   as max_feels_like_f,
    round(avg(humidity_pct), 1)                         as avg_humidity_pct,
    round(sum(precip_in), 2)                            as total_precip_in,
    sum(case when precip_mm > 0 then 1 else 0 end)      as rain_hours,
    max(wind_mph)                                       as max_wind_mph,
    count(*)                                            as hours_observed
from hourly
group by city_id, weather_date
