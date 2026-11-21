"""Central configuration for the pipeline.

The city list lives in dbt/seeds/cities.csv so the ingestion code and the
dbt models share one source of truth.
"""
from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CITIES_CSV = PROJECT_ROOT / "dbt" / "seeds" / "cities.csv"
DUCKDB_PATH = Path(os.getenv("DUCKDB_PATH", PROJECT_ROOT / "data" / "weather.duckdb"))

API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Hourly variables requested from Open-Meteo (metric units, UTC timestamps).
HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "relative_humidity_2m",
    "precipitation",
    "wind_speed_10m",
    "weather_code",
]

# The archive API lags real time by a few days.
ARCHIVE_LAG_DAYS = 5
# How far back the first run (backfill) goes.
DEFAULT_BACKFILL_DAYS = 730
# Max days per API request; larger ranges are split into chunks.
CHUNK_DAYS = 365
# Pause between requests to stay well inside the free-tier rate limit.
REQUEST_PAUSE_SECONDS = 1.0


@dataclass(frozen=True)
class City:
    city_id: str
    city_name: str
    latitude: float
    longitude: float


def load_cities(path: Path = CITIES_CSV) -> list[City]:
    with open(path, newline="") as f:
        return [
            City(r["city_id"], r["city_name"], float(r["latitude"]), float(r["longitude"]))
            for r in csv.DictReader(f)
        ]


def latest_available_date(today: date | None = None) -> date:
    return (today or date.today()) - timedelta(days=ARCHIVE_LAG_DAYS)
