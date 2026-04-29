CREATE VIEW top_tracks AS
SELECT
    track_name,
    popularity,
    duration_ms
FROM tracks
ORDER BY popularity DESC;

CREATE VIEW genre_popularity AS
SELECT
    g.genre_name,
    AVG(t.popularity) AS avg_popularity
FROM tracks t
JOIN genres g ON t.genre_id = g.genre_id
GROUP BY g.genre_name;

CREATE VIEW album_duration AS
SELECT
    a.album_name,
    SUM(t.duration_ms) AS total_duration
FROM tracks t
JOIN albums a ON t.album_id = a.album_id
GROUP BY a.album_name;