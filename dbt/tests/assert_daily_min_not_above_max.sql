-- A day's low temperature can never be above its high.
select *
from {{ ref('fct_daily_weather') }}
where min_temp_f > max_temp_f
