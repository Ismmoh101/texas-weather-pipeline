from datetime import date

import pandas as pd
import pytest

from pipeline import run as run_mod
from pipeline.config import HOURLY_VARIABLES, City, load_cities
from pipeline.extract import date_chunks, fetch_city, parse_response
from pipeline.load import DuckDBWarehouse
from pipeline.quality import validate

CITY = City("irving", "Irving", 32.814, -96.9489)


def fake_payload(start: str, hours: int, temp: float = 20.0) -> dict:
    times = pd.date_range(start, periods=hours, freq="h").strftime("%Y-%m-%dT%H:%M").tolist()
    hourly = {"time": times}
    for v in HOURLY_VARIABLES:
        hourly[v] = [temp if v != "weather_code" else 1] * hours
    return {"hourly": hourly}


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self.payload


class FakeSession:
    """Returns a payload covering exactly the requested date range."""

    def __init__(self):
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append(params)
        s, e = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
        return FakeResponse(fake_payload(params["start_date"], ((e - s).days + 1) * 24))


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr("pipeline.extract.time.sleep", lambda _: None)


def test_cities_seed_loads():
    ids = [c.city_id for c in load_cities()]
    assert "irving" in ids and len(ids) == len(set(ids))


def test_date_chunks_cover_range_without_gaps():
    chunks = list(date_chunks(date(2024, 1, 1), date(2025, 6, 30), size=365))
    assert chunks[0][0] == date(2024, 1, 1) and chunks[-1][1] == date(2025, 6, 30)
    for (_, a_end), (b_start, _) in zip(chunks, chunks[1:]):
        assert (b_start - a_end).days == 1


def test_parse_response_shape():
    df = parse_response(fake_payload("2025-01-01", 24), CITY)
    assert len(df) == 24
    assert list(df.columns) == ["city_id", "observed_at", *HOURLY_VARIABLES]


def test_parse_response_rejects_bad_payload():
    with pytest.raises(ValueError):
        parse_response({"error": True}, CITY)


def test_fetch_city_chunks_long_ranges():
    session = FakeSession()
    df = fetch_city(CITY, date(2022, 1, 1), date(2023, 12, 31), session)
    assert len(session.calls) == 2
    assert len(df) == 730 * 24


def test_validate_drops_bad_rows():
    df = parse_response(fake_payload("2025-01-01", 5), CITY)
    df = pd.concat([df, df.iloc[[0]]], ignore_index=True)          # duplicate
    df.loc[1, "temperature_2m"] = 99                                # out of range (°C)
    df.loc[2, list(df.columns[2:])] = None                          # empty hour
    clean, report = validate(df)
    assert (report.duplicates, report.out_of_range, report.all_null_measurements) == (1, 1, 1)
    assert len(clean) == report.rows_out == 3


def test_upsert_is_idempotent(tmp_path):
    wh = DuckDBWarehouse(tmp_path / "w.duckdb")
    df = parse_response(fake_payload("2025-01-01", 48), CITY)
    df["ingested_at"] = pd.Timestamp("2025-01-03")
    wh.upsert(df)
    df["temperature_2m"] = 25.0
    wh.upsert(df)  # same keys again, new values
    rows, temp = wh.con.execute(
        "select count(*), max(temperature_2m) from raw.weather_hourly").fetchone()
    assert rows == 48 and temp == 25.0
    assert wh.watermark("irving") == date(2025, 1, 2)
    wh.close()


def test_incremental_run_only_fetches_new_days(tmp_path, monkeypatch):
    monkeypatch.setattr(run_mod, "DUCKDB_PATH", tmp_path / "w.duckdb")
    session = FakeSession()
    monkeypatch.setattr(run_mod, "build_session", lambda: session)

    first = run_mod.run(start=date(2025, 1, 1), end=date(2025, 1, 10), city_ids=["irving"])
    assert first["rows_loaded"] == 240

    second = run_mod.run(end=date(2025, 1, 12), city_ids=["irving"])
    # Resumes from the watermark (Jan 10, re-pulled for late data) through Jan 12.
    assert session.calls[-1]["start_date"] == "2025-01-10"
    assert second["rows_extracted"] == 3 * 24

    wh = DuckDBWarehouse(tmp_path / "w.duckdb")
    total, runs = wh.con.execute(
        "select (select count(*) from raw.weather_hourly), (select count(*) from raw.pipeline_runs)").fetchone()
    assert total == 12 * 24 and runs == 2
    wh.close()
