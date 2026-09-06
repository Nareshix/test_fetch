import os
import duckdb

print(
    "[*] Building Master SQLite Database with Deduplicated FTS5 Search...", flush=True
)

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
has_anime = os.path.exists("anime.db")
if has_anime:
    conn.execute("ATTACH 'anime.db' AS anime_db (TYPE SQLITE);")
    conn.execute("""
        CREATE TABLE sqlite_db.anime AS SELECT * FROM anime_db.anime;
        CREATE TABLE sqlite_db.franchise_timeline AS SELECT * FROM anime_db.franchise_timeline;
        CREATE TABLE sqlite_db.anime_recommendation AS SELECT * FROM anime_db.anime_recommendation;
    """)

# 3. Create Look-up Indexes
print("[*] Building B-Tree indexes...", flush=True)
conn.execute("""
    CREATE UNIQUE INDEX idx_movies_imdb ON sqlite_db.movies(imdb_id);
    CREATE INDEX idx_movies_tmdb ON sqlite_db.movies(tmdb_id);
    CREATE UNIQUE INDEX idx_shows_imdb ON sqlite_db.shows(imdb_id);
    CREATE INDEX idx_shows_tmdb ON sqlite_db.shows(tmdb_id);
    CREATE INDEX idx_seasons_lookup ON sqlite_db.seasons(show_imdb_id, season_number);
    CREATE INDEX idx_seasons_tmdb ON sqlite_db.seasons(show_tmdb_id, season_number);
""")

if has_anime:
    conn.execute("""
        CREATE INDEX idx_anime_imdb ON sqlite_db.anime(imdb_id);
        CREATE INDEX idx_timeline_member ON sqlite_db.franchise_timeline(member_id);
        CREATE INDEX idx_timeline_root ON sqlite_db.franchise_timeline(root_id);
        CREATE INDEX idx_rec_anime ON sqlite_db.anime_recommendation(anime_id);
    """)

# 4. Build Unified FTS5 Search Index (Deduplicating Overlaps)
print("[*] Populating Unified FTS5 Search Index...", flush=True)
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
""")

if has_anime:
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

        INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'movie', imdb_id, year, poster_path, imdb_rating
        FROM sqlite_db.movies
        WHERE imdb_id NOT IN (SELECT imdb_id FROM sqlite_db.anime WHERE imdb_id LIKE 'tt%');

        INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'show', imdb_id, start_year, poster_path, imdb_rating
        FROM sqlite_db.shows
        WHERE imdb_id NOT IN (SELECT imdb_id FROM sqlite_db.anime WHERE imdb_id LIKE 'tt%');
    """)
else:
    conn.execute("""
        INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'movie', imdb_id, year, poster_path, imdb_rating FROM sqlite_db.movies;

        INSERT INTO sqlite_db.search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'show', imdb_id, start_year, poster_path, imdb_rating FROM sqlite_db.shows;
    """)

conn.close()
print("[+] Master SQLite Database created successfully at 'media.sqlite'!", flush=True)
