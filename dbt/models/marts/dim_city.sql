select
    city_id,
    city_name,
    county,
    latitude,
    longitude
from {{ ref('cities') }}
