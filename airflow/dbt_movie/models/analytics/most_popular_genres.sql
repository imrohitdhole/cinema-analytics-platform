WITH genre_data AS (
    SELECT
        movie_id,
        movie_title,
        popularity,
        genre.value AS genre_name
    FROM (
        SELECT
            movie_id,
            movie_title,
            popularity,
            SPLIT(genres, ',') AS genre_array
        FROM {{ ref('staging_imdb_tmdb_movies') }}
    ) t, 
    LATERAL FLATTEN(input => t.genre_array) AS genre
)
SELECT
    genre_name,
    COUNT(*) AS movie_count,
    AVG(popularity) AS avg_popularity
FROM genre_data
GROUP BY genre_name
ORDER BY avg_popularity DESC NULLS LAST  -- Snowflake sorts NULLs first in DESC by default