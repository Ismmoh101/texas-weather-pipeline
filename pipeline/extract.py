"""Extract: pull hourly weather history from the Open-Meteo archive API."""
from __future__ import annotations

import logging
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import API_URL, CHUNK_DAYS, HOURLY_VARIABLES, REQUEST_PAUSE_SECONDS, City

log = logging.getLogger(__name__)


def build_session() -> requests.Session:
    """HTTP session that retries transient failures with exponential backoff."""
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def date_chunks(start: date, end: date, size: int = CHUNK_DAYS):
    """Split [start, end] into consecutive ranges of at most `size` days."""
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=size - 1), end)
        yield cur, chunk_end
        cur = chunk_end + timedelta(days=1)


def parse_response(payload: dict, city: City) -> pd.DataFrame:
    """Turn the API's column-oriented JSON into one row per city-hour."""
    hourly = payload.get("hourly") or {}
    if "time" not in hourly:
        raise ValueError(f"Unexpected API response for {city.city_id}: no hourly.time")

    df = pd.DataFrame({"observed_at": pd.to_datetime(hourly["time"])})
    for var in HOURLY_VARIABLES:
        df[var] = hourly.get(var, [None] * len(df))
    df.insert(0, "city_id", city.city_id)
    return df


def fetch_city(
    city: City, start: date, end: date, session: requests.Session | None = None
) -> pd.DataFrame:
    """Fetch every hour between start and end (inclusive) for one city."""
    session = session or build_session()
    frames = []
    for chunk_start, chunk_end in date_chunks(start, end):
        params = {
            "latitude": city.latitude,
            "longitude": city.longitude,
            "start_date": chunk_start.isoformat(),
            "end_date": chunk_end.isoformat(),
            "hourly": ",".join(HOURLY_VARIABLES),
            "timezone": "GMT",
        }
        log.info("Fetching %s %s -> %s", city.city_id, chunk_start, chunk_end)
        resp = session.get(API_URL, params=params, timeout=60)
        resp.raise_for_status()
        frames.append(parse_response(resp.json(), city))
        time.sleep(REQUEST_PAUSE_SECONDS)

    if not frames:
        return pd.DataFrame(columns=["city_id", "observed_at", *HOURLY_VARIABLES])
    df = pd.concat(frames, ignore_index=True)
    df["ingested_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    return df
