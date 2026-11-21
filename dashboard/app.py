"""Streamlit dashboard on top of the dbt marts.

Run from the repo root:  streamlit run dashboard/app.py
"""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import streamlit as st

DB = Path(os.getenv("DUCKDB_PATH", Path(__file__).resolve().parent.parent / "data" / "weather.duckdb"))

st.set_page_config(page_title="Texas Weather Pipeline", layout="wide")
st.title("Texas Weather Pipeline")


@st.cache_data(ttl=600)
def query(sql: str, params: list | None = None):
    with duckdb.connect(str(DB), read_only=True) as con:
        return con.execute(sql, params or []).df()


if not DB.exists():
    st.error(f"No warehouse at {DB}. Run `python -m pipeline.run` and `dbt build` first.")
    st.stop()

cities = query("select city_id, city_name from marts.dim_city order by city_name")
names = dict(zip(cities.city_name, cities.city_id))
picked = st.sidebar.multiselect("Cities", list(names), default=list(names)[:3])
ids = [names[n] for n in picked] or list(names.values())

bounds = query("select min(weather_date) lo, max(weather_date) hi from marts.fct_daily_weather")
lo, hi = bounds.lo[0], bounds.hi[0]
start, end = st.sidebar.date_input("Date range", (lo, hi), min_value=lo, max_value=hi)

placeholders = ",".join("?" * len(ids))
daily = query(
    f"""select d.weather_date, c.city_name, d.*
        from marts.fct_daily_weather d join marts.dim_city c using (city_id)
        where d.city_id in ({placeholders}) and d.weather_date between ? and ?
        order by d.weather_date""",
    [*ids, start, end],
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Days covered", f"{daily.weather_date.nunique():,}")
k2.metric("Hottest high", f"{daily.max_temp_f.max():.0f}°F" if len(daily) else "–")
k3.metric("Coldest low", f"{daily.min_temp_f.min():.0f}°F" if len(daily) else "–")
k4.metric("Total rain", f"{daily.total_precip_in.sum():.1f} in" if len(daily) else "–")

st.subheader("Daily high (°F)")
st.line_chart(daily.pivot_table(index="weather_date", columns="city_name", values="max_temp_f"))

st.subheader("Monthly rainfall (in)")
monthly = daily.assign(month=daily.weather_date.astype("datetime64[ns]").dt.to_period("M").dt.to_timestamp())
st.bar_chart(monthly.pivot_table(index="month", columns="city_name", values="total_precip_in", aggfunc="sum"))

st.subheader("Heat events (100°F+ streaks)")
events = query(
    f"""select c.city_name, e.event_start, e.event_end, e.duration_days, e.peak_temp_f
        from marts.fct_heat_events e join marts.dim_city c using (city_id)
        where e.city_id in ({placeholders}) and e.event_start between ? and ?
        order by e.duration_days desc, e.event_start""",
    [*ids, start, end],
)
st.dataframe(events, width="stretch", hide_index=True)

with st.expander("Pipeline run log"):
    st.dataframe(query("select * from raw.pipeline_runs order by started_at desc limit 20"),
                 width="stretch", hide_index=True)
