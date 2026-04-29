DELIMITER //

CREATE PROCEDURE get_top_tracks(IN limit_count INT)
BEGIN
    SELECT track_name, popularity
    FROM tracks
    ORDER BY popularity DESC
    LIMIT limit_count;
END //

CREATE PROCEDURE genre_stats(IN genre_name_input VARCHAR(100))
BEGIN
    SELECT
        g.genre_name,
        COUNT(t.track_id) AS total_tracks,
        AVG(t.popularity) AS avg_popularity
    FROM tracks t
    JOIN genres g ON t.genre_id = g.genre_id
    WHERE g.genre_name = genre_name_input
    GROUP BY g.genre_name;
END //

DELIMITER ;