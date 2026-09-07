import os
import sqlite3
import duckdb

print(
    "[*] Building Master SQLite Database with Deduplicated FTS5 Search...", flush=True
)

if os.path.exists("media.sqlite"):
    os.remove("media.sqlite")

# 1. Use DuckDB for fast CSV loading into SQLite
conn = duckdb.connect()
conn.execute("INSTALL sqlite; LOAD sqlite;")
conn.execute("ATTACH 'media.sqlite' AS sqlite_db (TYPE SQLITE);")

print("[*] Ingesting CSV datasets into SQLite tables...", flush=True)
conn.execute("""
    CREATE TABLE sqlite_db.movies AS SELECT * FROM 'movies.csv.gz';
    CREATE TABLE sqlite_db.shows AS SELECT * FROM 'shows.csv.gz';
    CREATE TABLE sqlite_db.seasons AS SELECT * FROM 'seasons.csv.gz';
""")

# 2. Attach Anime database if present and copy tables
has_anime = os.path.exists("anime.db")
if has_anime:
    print("[*] Copying anime datasets...", flush=True)
    conn.execute("ATTACH 'anime.db' AS anime_db (TYPE SQLITE);")
    conn.execute("""
        CREATE TABLE sqlite_db.anime AS SELECT * FROM anime_db.anime;
        CREATE TABLE sqlite_db.franchise_timeline AS SELECT * FROM anime_db.franchise_timeline;
        CREATE TABLE sqlite_db.anime_recommendation AS SELECT * FROM anime_db.anime_recommendation;
    """)

# Close DuckDB connection so sqlite3 can acquire exclusive locks
conn.close()

# 3. Connect using native SQLite for indexes and FTS5
print("[*] Connecting via sqlite3 to build indexes and FTS5...", flush=True)
s_conn = sqlite3.connect("media.sqlite")
cur = s_conn.cursor()

print("[*] Building B-Tree indexes...", flush=True)
cur.executescript("""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_movies_imdb ON movies(imdb_id);
    CREATE INDEX IF NOT EXISTS idx_movies_tmdb ON movies(tmdb_id);
    CREATE UNIQUE INDEX IF NOT EXISTS idx_shows_imdb ON shows(imdb_id);
    CREATE INDEX IF NOT EXISTS idx_shows_tmdb ON shows(tmdb_id);
    CREATE INDEX IF NOT EXISTS idx_seasons_lookup ON seasons(show_imdb_id, season_number);
    CREATE INDEX IF NOT EXISTS idx_seasons_tmdb ON seasons(show_tmdb_id, season_number);
""")

if has_anime:
    cur.executescript("""
        CREATE INDEX IF NOT EXISTS idx_anime_imdb ON anime(imdb_id);
        CREATE INDEX IF NOT EXISTS idx_timeline_member ON franchise_timeline(member_id);
        CREATE INDEX IF NOT EXISTS idx_timeline_root ON franchise_timeline(root_id);
        CREATE INDEX IF NOT EXISTS idx_rec_anime ON anime_recommendation(anime_id);
    """)

# 4. Build Unified FTS5 Search Index
print("[*] Populating Unified FTS5 Search Index...", flush=True)
cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS search_index USING fts5(
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
    cur.executescript("""
        INSERT INTO search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT
            COALESCE(title_english, title_romaji) AS title,
            title_romaji AS original_title,
            studio AS casts,
            'anime' AS media_type,
            imdb_id AS item_id,
            CAST(SUBSTR(start_date, 1, 4) AS INTEGER) AS year,
            cover_url AS poster_path,
            rating
        FROM anime;

        INSERT INTO search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'movie', imdb_id, year, poster_path, imdb_rating
        FROM movies
        WHERE imdb_id NOT IN (SELECT imdb_id FROM anime WHERE imdb_id LIKE 'tt%');

        INSERT INTO search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'show', imdb_id, start_year, poster_path, imdb_rating
        FROM shows
        WHERE imdb_id NOT IN (SELECT imdb_id FROM anime WHERE imdb_id LIKE 'tt%');
    """)
else:
    cur.executescript("""
        INSERT INTO search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'movie', imdb_id, year, poster_path, imdb_rating FROM movies;

        INSERT INTO search_index (title, original_title, casts, media_type, item_id, year, poster_path, rating)
        SELECT title, original_title, casts, 'show', imdb_id, start_year, poster_path, imdb_rating FROM shows;
    """)

s_conn.commit()
s_conn.close()
print("[+] Master SQLite Database created successfully at 'media.sqlite'!", flush=True)
