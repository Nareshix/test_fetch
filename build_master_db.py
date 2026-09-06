import os
import duckdb

print("[*] Building unified Master SQLite Database from CSV tables...", flush=True)

if os.path.exists("media.sqlite"):
    os.remove("media.sqlite")

conn = duckdb.connect()
conn.execute("INSTALL sqlite; LOAD sqlite;")
conn.execute("ATTACH 'media.sqlite' AS sqlite_db (TYPE SQLITE);")

# 1. Attach and load Movies, Shows, Seasons
conn.execute("""
    CREATE TABLE sqlite_db.movies AS SELECT * FROM 'movies.csv.gz';
    CREATE TABLE sqlite_db.shows AS SELECT * FROM 'shows.csv.gz';
    CREATE TABLE sqlite_db.seasons AS SELECT * FROM 'seasons.csv.gz';
""")

# 2. Attach Anime database if present
if os.path.exists("media.db"):
    conn.execute("ATTACH 'media.db' AS anime_db (TYPE SQLITE);")
    conn.execute("""
        CREATE TABLE sqlite_db.anime AS SELECT * FROM anime_db.anime;
        CREATE TABLE sqlite_db.franchise_timeline AS SELECT * FROM anime_db.franchise_timeline;
        CREATE TABLE sqlite_db.anime_recommendation AS SELECT * FROM anime_db.anime_recommendation;
    """)

# 3. Create Indexes for Lookups
conn.execute("""
    CREATE UNIQUE INDEX idx_movies_imdb ON sqlite_db.movies(imdb_id);
    CREATE INDEX idx_movies_tmdb ON sqlite_db.movies(tmdb_id);
    CREATE UNIQUE INDEX idx_shows_imdb ON sqlite_db.shows(imdb_id);
    CREATE INDEX idx_shows_tmdb ON sqlite_db.shows(tmdb_id);
    CREATE INDEX idx_seasons_lookup ON sqlite_db.seasons(show_imdb_id, season_number);
    CREATE INDEX idx_seasons_tmdb ON sqlite_db.seasons(show_tmdb_id, season_number);
""")

# 4. Build Unified FTS5 Search Index
conn.execute("""
    CREATE VIRTUAL TABLE sqlite_db.search_index USING fts5(
        title,
        original_title,
        casts,
        media_type UNINDEXED,
        item_id UNINDEXED,
        year UNINDEXED,
        poster_path UNINDEXED,
        rating UNINDEXED
    );

    INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
    SELECT title, original_title, casts, 'movie', imdb_id, year, poster_path, imdb_rating FROM sqlite_db.movies;

    INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
    SELECT title, original_title, casts, 'show', imdb_id, start_year, poster_path, imdb_rating FROM sqlite_db.shows;
""")

if os.path.exists("media.db"):
    conn.execute("""
        INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT
            COALESCE(title_english, title_romaji) AS title,
            title_romaji AS original_title,
            studio AS casts,
            'anime' AS media_type,
            imdb_id AS item_id,
            TRY_CAST(SUBSTRING(start_date, 1, 4) AS INTEGER) AS year,
            cover_url AS poster_path,
            rating
        FROM sqlite_db.anime;
    """)

conn.close()
print(
    "[+] Master SQLite Database built at 'media.sqlite' with sub-millisecond FTS5 search!",
    flush=True,
)
