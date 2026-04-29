-- tracks
CREATE INDEX idx_tracks_album ON tracks(album_id);
CREATE INDEX idx_tracks_genre ON tracks(genre_id);

-- track_artist
CREATE INDEX idx_track_artist_track ON track_artist(track_id);
CREATE INDEX idx_track_artist_artist ON track_artist(artist_id);

-- audio_features
CREATE INDEX idx_audio_track ON audio_features(track_id);