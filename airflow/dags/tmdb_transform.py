"""Transformation and loading helpers for the TMDb pipeline.

This module has no Airflow or Snowflake imports, so the logic can be unit
tested on its own (see airflow/tests/test_tmdb_transform.py). The TMDb DAG in
tmdb_realtime.py calls these functions from its tasks.
"""
import math
from datetime import datetime, timedelta

TARGET_TABLE = "movies.raw_data.tmdb_data_movies"

GENRE_MAPPING = {
    28: "Action",
    12: "Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    9648: "Mystery",
    10749: "Romance",
    878: "Science Fiction",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
    37: "Western",
}

COLUMNS = (
    "movie_id",
    "title",
    "release_date",
    "original_language",
    "popularity",
    "vote_average",
    "vote_count",
    "genre_ids",
)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {table} (
    movie_id INTEGER PRIMARY KEY,
    title VARCHAR,
    release_date DATE,
    original_language VARCHAR,
    popularity FLOAT,
    vote_average FLOAT,
    vote_count INTEGER,
    genre_ids TEXT
)
"""

# Values are bound as parameters (pyformat, the Snowflake connector default),
# so None is sent as NULL and strings are escaped by the driver.
MERGE_SQL = """
MERGE INTO {table} AS target
USING (SELECT %(movie_id)s AS movie_id,
              %(title)s AS title,
              TO_DATE(%(release_date)s, 'YYYY-MM-DD') AS release_date,
              %(original_language)s AS original_language,
              %(popularity)s::FLOAT AS popularity,
              %(vote_average)s::FLOAT AS vote_average,
              %(vote_count)s::INTEGER AS vote_count,
              %(genre_ids)s AS genre_ids) AS source
ON target.movie_id = source.movie_id
WHEN MATCHED THEN UPDATE
    SET title = source.title,
        release_date = source.release_date,
        original_language = source.original_language,
        popularity = source.popularity,
        vote_average = source.vote_average,
        vote_count = source.vote_count,
        genre_ids = source.genre_ids
WHEN NOT MATCHED THEN INSERT
    (movie_id, title, release_date, original_language, popularity, vote_average, vote_count, genre_ids)
    VALUES (source.movie_id, source.title, source.release_date, source.original_language,
            source.popularity, source.vote_average, source.vote_count, source.genre_ids)
"""


def clean_metric(value):
    """Return a valid non-negative measurement, or None when it is unknown.

    Zero is a real measurement (a new release with no votes yet) and is kept.
    Missing, non-numeric, boolean, NaN, infinite and negative values return
    None, which is loaded into Snowflake as NULL.
    """
    if value is None or isinstance(value, bool):
        return None
    if not isinstance(value, (int, float)):
        return None
    if math.isnan(value) or math.isinf(value) or value < 0:
        return None
    return value


def clean_count(value):
    """Like clean_metric, but also requires a whole number; returns an int."""
    value = clean_metric(value)
    if value is None or not float(value).is_integer():
        return None
    return int(value)


def parse_release_date(value):
    """Parse a TMDb YYYY-MM-DD release date, or return None if missing or malformed."""
    if not isinstance(value, str):
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return None


def map_genres(genre_ids):
    """Turn TMDb genre IDs into a comma-separated list of names (unknown IDs kept as-is)."""
    return ",".join(GENRE_MAPPING.get(genre_id, str(genre_id)) for genre_id in (genre_ids or []))


def transform_movies(data, now=None):
    """Keep movies released in the last 7 days and select the fields we load."""
    if not data:
        return []

    now = now or datetime.now()
    one_week_ago = now - timedelta(days=7)

    transformed = []
    for movie in data:
        release_date = parse_release_date(movie.get("release_date"))
        if release_date is None or release_date < one_week_ago:
            continue

        transformed.append({
            "movie_id": movie.get("id"),
            "title": movie.get("title"),
            "release_date": movie.get("release_date"),
            "original_language": movie.get("original_language"),
            "popularity": clean_metric(movie.get("popularity")),
            "vote_average": clean_metric(movie.get("vote_average")),
            "vote_count": clean_count(movie.get("vote_count")),
            "genre_ids": map_genres(movie.get("genre_ids")),
        })
    return transformed


def merge_params(record):
    """Build the bound parameters for one MERGE; None values become NULL."""
    return {column: record.get(column) for column in COLUMNS}


def load_records(cursor, records, table=TARGET_TABLE):
    """MERGE each record into the target table with bound parameters."""
    sql = MERGE_SQL.format(table=table)
    for record in records:
        cursor.execute(sql, merge_params(record))
    return len(records)
