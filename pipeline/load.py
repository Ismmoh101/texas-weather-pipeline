"""Load: idempotent upserts into DuckDB (local) or Snowflake (cloud)."""
from __future__ import annotations

import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from .config import HOURLY_VARIABLES

log = logging.getLogger(__name__)

RAW_COLUMNS = ["city_id", "observed_at", *HOURLY_VARIABLES, "ingested_at"]

DDL = """
create schema if not exists raw;

create table if not exists raw.weather_hourly (
    city_id               varchar   not null,
    observed_at           timestamp not null,
    temperature_2m        double,
    apparent_temperature  double,
    relative_humidity_2m  double,
    precipitation         double,
    wind_speed_10m        double,
    weather_code          integer,
    ingested_at           timestamp not null,
    primary key (city_id, observed_at)
);

create table if not exists raw.pipeline_runs (
    run_id          varchar   not null,
    started_at      timestamp not null,
    finished_at     timestamp,
    cities          integer,
    rows_extracted  integer,
    rows_rejected   integer,
    rows_loaded     integer,
    status          varchar
);
"""


class DuckDBWarehouse:
    def __init__(self, path: Path):
        import duckdb

        path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(path))
        self.con.execute(DDL)

    def watermark(self, city_id: str) -> date | None:
        """Latest date already loaded for a city (drives incremental loads)."""
        row = self.con.execute(
            "select max(observed_at)::date from raw.weather_hourly where city_id = ?", [city_id]
        ).fetchone()
        return row[0] if row else None

    def upsert(self, df: pd.DataFrame) -> int:
        """Insert new rows and overwrite existing (city_id, observed_at) rows."""
        if df.empty:
            return 0
        batch = df[RAW_COLUMNS]  # noqa: F841  (referenced by name in the SQL below)
        self.con.execute(f"insert or replace into raw.weather_hourly select {', '.join(RAW_COLUMNS)} from batch")
        return len(df)

    def log_run(self, run: dict) -> None:
        self.con.execute(
            "insert into raw.pipeline_runs values (?, ?, ?, ?, ?, ?, ?, ?)",
            [run[k] for k in ("run_id", "started_at", "finished_at", "cities",
                              "rows_extracted", "rows_rejected", "rows_loaded", "status")],
        )

    def close(self) -> None:
        self.con.close()


class SnowflakeWarehouse:
    """Same interface as DuckDBWarehouse, backed by Snowflake.

    Credentials come from environment variables (see .env.example).
    Rows are bulk-loaded into a temporary table with write_pandas, then
    MERGEd into raw.weather_hourly so reruns never create duplicates.
    """

    def __init__(self):
        import snowflake.connector

        self.con = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            database=os.environ.get("SNOWFLAKE_DATABASE", "WEATHER"),
            role=os.environ.get("SNOWFLAKE_ROLE"),
        )
        cur = self.con.cursor()
        # Snowflake records but does not enforce primary keys; the MERGE in
        # upsert() is what guarantees one row per (city_id, observed_at).
        for stmt in DDL.split(";"):
            if stmt.strip():
                cur.execute(stmt)

    def watermark(self, city_id: str) -> date | None:
        cur = self.con.cursor()
        cur.execute("select max(observed_at)::date from raw.weather_hourly where city_id = %s", (city_id,))
        row = cur.fetchone()
        return row[0] if row else None

    def upsert(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        from snowflake.connector.pandas_tools import write_pandas

        cur = self.con.cursor()
        cur.execute("create or replace temporary table raw.weather_hourly_stage like raw.weather_hourly")
        stage_df = df[RAW_COLUMNS].copy()
        stage_df.columns = [c.upper() for c in stage_df.columns]
        write_pandas(self.con, stage_df, "WEATHER_HOURLY_STAGE", schema="RAW", use_logical_type=True)

        measures = [c for c in RAW_COLUMNS if c not in ("city_id", "observed_at")]
        cur.execute(f"""
            merge into raw.weather_hourly t
            using raw.weather_hourly_stage s
              on t.city_id = s.city_id and t.observed_at = s.observed_at
            when matched then update set {', '.join(f't.{c} = s.{c}' for c in measures)}
            when not matched then insert ({', '.join(RAW_COLUMNS)})
                 values ({', '.join(f's.{c}' for c in RAW_COLUMNS)})
        """)
        return len(df)

    def log_run(self, run: dict) -> None:
        cur = self.con.cursor()
        cur.execute(
            "insert into raw.pipeline_runs values (%s, %s, %s, %s, %s, %s, %s, %s)",
            tuple(run[k] for k in ("run_id", "started_at", "finished_at", "cities",
                                   "rows_extracted", "rows_rejected", "rows_loaded", "status")),
        )

    def close(self) -> None:
        self.con.close()


def get_warehouse(kind: str, duckdb_path: Path):
    if kind == "duckdb":
        return DuckDBWarehouse(duckdb_path)
    if kind == "snowflake":
        return SnowflakeWarehouse()
    raise ValueError(f"Unknown warehouse: {kind}")


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
