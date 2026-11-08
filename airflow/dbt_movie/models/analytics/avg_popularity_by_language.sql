-- models/analytics/avg_popularity_by_language.sql

WITH language_popularity AS (
    SELECT
        original_language,
        AVG(popularity) AS avg_popularity,
        COUNT(*) AS movie_count
    FROM {{ ref('staging_imdb_tmdb_movies') }}
    GROUP BY original_language
)
SELECT
    original_language,
    avg_popularity,
    movie_count
FROM language_popularity
ORDER BY avg_popularity DESC NULLS LAST  -- Snowflake sorts NULLs first in DESC by default