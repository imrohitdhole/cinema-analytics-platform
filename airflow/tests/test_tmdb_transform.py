"""Regression tests for TMDb metric handling.

Earlier versions replaced missing *and* zero popularity/vote values with random
integers. These tests pin the corrected behaviour: zeros are kept, unknown or
invalid values become None (NULL in Snowflake), and nothing is random.
"""
import math
from datetime import datetime
from pathlib import Path

import pytest

import tmdb_transform as tt

NOW = datetime(2024, 12, 8, 12, 0, 0)
RECENT = "2024-12-05"   # inside the 7-day window
OLD = "2024-11-20"      # outside the window
DAG_FILE = Path(__file__).resolve().parents[1] / "dags" / "tmdb_realtime.py"


def movie(**overrides):
    base = {
        "id": 101,
        "title": "Example Movie",
        "release_date": RECENT,
        "original_language": "en",
        "popularity": 42.5,
        "vote_average": 7.1,
        "vote_count": 250,
        "genre_ids": [28, 12],
    }
    base.update(overrides)
    return base


def only(records):
    assert len(records) == 1
    return records[0]


# --- zeros are real measurements -------------------------------------------

def test_zero_metrics_are_preserved():
    record = only(tt.transform_movies([movie(popularity=0, vote_average=0.0, vote_count=0)], now=NOW))
    assert record["popularity"] == 0
    assert record["vote_average"] == 0.0
    assert record["vote_count"] == 0
    assert isinstance(record["vote_count"], int)


@pytest.mark.parametrize("value", [0, 0.0])
def test_clean_metric_keeps_zero(value):
    assert tt.clean_metric(value) == 0
    assert tt.clean_metric(value) is not None


# --- unknown values become None (NULL) --------------------------------------

def test_missing_metric_keys_become_none():
    raw = movie()
    for key in ("popularity", "vote_average", "vote_count"):
        raw.pop(key)
    record = only(tt.transform_movies([raw], now=NOW))
    assert record["popularity"] is None
    assert record["vote_average"] is None
    assert record["vote_count"] is None


def test_explicit_none_metrics_stay_none():
    record = only(tt.transform_movies([movie(popularity=None, vote_average=None, vote_count=None)], now=NOW))
    assert (record["popularity"], record["vote_average"], record["vote_count"]) == (None, None, None)


@pytest.mark.parametrize("value", [
    "", "abc", "7.5", True, False, [], {},
    float("nan"), float("inf"), float("-inf"),
    -1, -0.5,
])
def test_invalid_metric_values_become_none(value):
    assert tt.clean_metric(value) is None


@pytest.mark.parametrize("value", [0.001, 7.25, 1, 1234, 98765.4321])
def test_valid_metric_values_pass_through_unchanged(value):
    assert tt.clean_metric(value) == value


@pytest.mark.parametrize("value, expected", [
    (0, 0), (12, 12), (12.0, 12), (12.5, None), (-3, None), (None, None), ("12", None),
])
def test_clean_count(value, expected):
    result = tt.clean_count(value)
    assert result == expected
    if expected is not None:
        assert isinstance(result, int)


# --- no randomness ----------------------------------------------------------

def test_transform_is_deterministic():
    data = [movie(popularity=None, vote_count=None), movie(id=102, popularity=0, vote_average=None)]
    assert tt.transform_movies(data, now=NOW) == tt.transform_movies(data, now=NOW)


def test_random_imputation_is_gone():
    assert not hasattr(tt, "random")
    dag_source = DAG_FILE.read_text()
    assert "import random" not in dag_source
    assert "generate_random_value" not in dag_source


# --- release-date window and other fields -----------------------------------

def test_only_movies_from_last_seven_days_are_kept():
    data = [movie(id=1, release_date=RECENT), movie(id=2, release_date=OLD)]
    assert [r["movie_id"] for r in tt.transform_movies(data, now=NOW)] == [1]


@pytest.mark.parametrize("release_date", [None, "", "not-a-date", "2024/12/05"])
def test_missing_or_malformed_release_dates_are_skipped(release_date):
    assert tt.transform_movies([movie(release_date=release_date)], now=NOW) == []


def test_empty_input_returns_empty_list():
    assert tt.transform_movies([], now=NOW) == []
    assert tt.transform_movies(None, now=NOW) == []


@pytest.mark.parametrize("genre_ids, expected", [
    ([28, 12], "Action,Adventure"),
    ([28, 99999], "Action,99999"),
    ([], ""),
    (None, ""),
])
def test_genre_mapping(genre_ids, expected):
    assert tt.map_genres(genre_ids) == expected


def test_selected_fields():
    record = only(tt.transform_movies([movie()], now=NOW))
    assert record == {
        "movie_id": 101,
        "title": "Example Movie",
        "release_date": RECENT,
        "original_language": "en",
        "popularity": 42.5,
        "vote_average": 7.1,
        "vote_count": 250,
        "genre_ids": "Action,Adventure",
    }


# --- loading: None is sent as NULL via bound parameters ---------------------

class FakeCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))


def render_like_pyformat(sql, params):
    """Mimic the Snowflake connector's client-side pyformat binding."""
    def literal(value):
        if value is None:
            return "NULL"
        if isinstance(value, str):
            return "'" + value.replace("'", "''") + "'"
        return repr(value)
    return sql % {key: literal(value) for key, value in params.items()}


def test_load_binds_none_as_parameter_not_text():
    records = tt.transform_movies([movie(popularity=None, vote_average=0.0, vote_count=None)], now=NOW)
    cursor = FakeCursor()

    assert tt.load_records(cursor, records, table="t") == 1
    sql, params = cursor.calls[0]

    assert params["popularity"] is None
    assert params["vote_count"] is None
    assert params["vote_average"] == 0.0
    assert "None" not in sql            # no Python None leaks into the SQL text
    assert "%(popularity)s" in sql


def test_merge_renders_null_and_zero_correctly():
    record = only(tt.transform_movies(
        [movie(title="O'Brien's Story", popularity=None, vote_average=0.0, vote_count=0)], now=NOW))
    rendered = render_like_pyformat(tt.MERGE_SQL.format(table="t"), tt.merge_params(record))

    assert "NULL::FLOAT AS popularity" in rendered
    assert "0.0::FLOAT AS vote_average" in rendered
    assert "0::INTEGER AS vote_count" in rendered
    assert "'O''Brien''s Story' AS title" in rendered


def test_load_executes_one_merge_per_record():
    records = tt.transform_movies([movie(id=1), movie(id=2), movie(id=3)], now=NOW)
    cursor = FakeCursor()
    tt.load_records(cursor, records, table="t")
    assert [params["movie_id"] for _, params in cursor.calls] == [1, 2, 3]
