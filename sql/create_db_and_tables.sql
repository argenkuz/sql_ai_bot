CREATE DATABASE spotify_db;

USE spotify_db;



CREATE TABLE artists (
    artist_id INT PRIMARY KEY,
    artist_name VARCHAR(255)
);

CREATE TABLE albums (
    album_id INT PRIMARY KEY,
    album_name VARCHAR(255)
);


CREATE TABLE genres (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(100)
);

CREATE TABLE tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    track_name VARCHAR(255),

    album_id INT,
    genre_id INT,

    popularity INT,
    duration_ms INT,
    is_explicit int,

    FOREIGN KEY (album_id) REFERENCES albums(album_id),
    FOREIGN KEY (genre_id) REFERENCES genres(genre_id)

);


CREATE TABLE track_artist (
    id INT PRIMARY KEY,
    track_id VARCHAR(50),
    artist_id INT,

    FOREIGN KEY (track_id) REFERENCES tracks(track_id),
    FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
);


CREATE TABLE audio_features (
    feature_id INT PRIMARY KEY,
    track_id VARCHAR(50),

    danceability FLOAT,
    energy FLOAT,
    `key` INT,
    loudness FLOAT,
    mode INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INT,

    FOREIGN KEY (track_id) REFERENCES tracks(track_id)
);