"""Data quality checks applied before anything reaches the warehouse."""
from __future__ import annotations

import logging
from dataclasses import dataclass

import pandas as pd

log = logging.getLogger(__name__)

# Physically plausible bounds (metric units). Rows outside them are rejected.
BOUNDS = {
    "temperature_2m": (-40, 60),
    "apparent_temperature": (-50, 70),
    "relative_humidity_2m": (0, 100),
    "precipitation": (0, 300),
    "wind_speed_10m": (0, 300),
}


@dataclass
class QualityReport:
    rows_in: int
    null_keys: int
    duplicates: int
    out_of_range: int
    all_null_measurements: int

    @property
    def rows_out(self) -> int:
        return self.rows_in - self.null_keys - self.duplicates - self.out_of_range - self.all_null_measurements


def validate(df: pd.DataFrame) -> tuple[pd.DataFrame, QualityReport]:
    rows_in = len(df)

    key_null = df["city_id"].isna() | df["observed_at"].isna()
    df = df[~key_null]

    dup = df.duplicated(subset=["city_id", "observed_at"], keep="last")
    df = df[~dup]

    bad = pd.Series(False, index=df.index)
    for col, (lo, hi) in BOUNDS.items():
        vals = df[col]
        bad |= vals.notna() & ((vals < lo) | (vals > hi))
    df = df[~bad]

    # Hours the API has not filled yet come back with every measurement null.
    empty = df[list(BOUNDS)].isna().all(axis=1)
    df = df[~empty]

    report = QualityReport(
        rows_in=rows_in,
        null_keys=int(key_null.sum()),
        duplicates=int(dup.sum()),
        out_of_range=int(bad.sum()),
        all_null_measurements=int(empty.sum()),
    )
    log.info("Quality check: %s", report)
    return df.reset_index(drop=True), report
