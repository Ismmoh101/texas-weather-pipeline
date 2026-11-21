{# Convert a naive UTC timestamp to naive US Central time, per warehouse. #}
{% macro to_local_time(column) -%}
    {{ return(adapter.dispatch('to_local_time')(column)) }}
{%- endmacro %}

{% macro default__to_local_time(column) -%}
    convert_timezone('UTC', 'America/Chicago', {{ column }})
{%- endmacro %}

{% macro duckdb__to_local_time(column) -%}
    (({{ column }} at time zone 'UTC') at time zone 'America/Chicago')
{%- endmacro %}
