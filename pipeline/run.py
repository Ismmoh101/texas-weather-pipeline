"""Run the extract -> validate -> load step.

Examples:
    python -m pipeline.run                         # incremental (backfills 2 years on first run)
    python -m pipeline.run --start 2024-01-01 --end 2024-12-31
    python -m pipeline.run --cities dallas irving --warehouse snowflake
"""
from __future__ import annotations

import argparse
import logging
import uuid
from datetime import date, timedelta

import pandas as pd

from .config import DEFAULT_BACKFILL_DAYS, DUCKDB_PATH, latest_available_date, load_cities
from .extract import build_session, fetch_city
from .load import get_warehouse, utcnow
from .quality import validate

log = logging.getLogger("pipeline")


def resolve_start(watermark: date | None, end: date) -> date:
    """Incremental logic: resume after the last loaded day.

    The last loaded day is re-pulled because the archive can fill in late
    hours; the upsert makes that safe.
    """
    if watermark is None:
        return end - timedelta(days=DEFAULT_BACKFILL_DAYS - 1)
    return watermark


def run(start: date | None = None, end: date | None = None, city_ids: list[str] | None = None,
        warehouse: str = "duckdb") -> dict:
    end = end or latest_available_date()
    cities = [c for c in load_cities() if not city_ids or c.city_id in city_ids]
    if not cities:
        raise SystemExit(f"No cities matched {city_ids}")

    wh = get_warehouse(warehouse, DUCKDB_PATH)
    session = build_session()
    run_info = {"run_id": uuid.uuid4().hex[:12], "started_at": utcnow(), "finished_at": None,
                "cities": len(cities), "rows_extracted": 0, "rows_rejected": 0, "rows_loaded": 0,
                "status": "running"}
    try:
        frames = []
        for city in cities:
            city_start = start or resolve_start(wh.watermark(city.city_id), end)
            if city_start > end:
                log.info("%s is up to date", city.city_id)
                continue
            frames.append(fetch_city(city, city_start, end, session))

        raw = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        run_info["rows_extracted"] = len(raw)
        if not raw.empty:
            clean, report = validate(raw)
            run_info["rows_rejected"] = report.rows_in - report.rows_out
            run_info["rows_loaded"] = wh.upsert(clean)
        run_info["status"] = "success"
        return run_info
    except Exception:
        run_info["status"] = "failed"
        raise
    finally:
        run_info["finished_at"] = utcnow()
        wh.log_run(run_info)
        wh.close()
        log.info("Run %s: %s", run_info["run_id"], run_info)


def main(argv: list[str] | None = None) -> dict:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--start", type=date.fromisoformat, help="YYYY-MM-DD (default: incremental)")
    p.add_argument("--end", type=date.fromisoformat, help="YYYY-MM-DD (default: latest available)")
    p.add_argument("--cities", nargs="*", help="city_ids from dbt/seeds/cities.csv (default: all)")
    p.add_argument("--warehouse", choices=["duckdb", "snowflake"], default="duckdb")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    return run(args.start, args.end, args.cities, args.warehouse)


if __name__ == "__main__":
    main()
