import os
import sqlite3
import urllib.request
import duckdb

FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "name.basics.tsv.gz",
]
BASE_URL = "https://datasets.imdbws.com/"


def download_progress(block_num, block_size, total_size):
    downloaded = block_num * block_size
    if total_size > 0:
        percent = (downloaded / total_size) * 100
        mb = downloaded / (1024 * 1024)
        total_mb = total_size / (1024 * 1024)
        print(
            f"\rDownloading: {percent:.1f}% ({mb:.1f}/{total_mb:.1f} MB)",
            end="",
            flush=True,
        )


def download_files():
    for filename in FILES:
        if not os.path.exists(filename):
            print(f"\nDownloading {filename}...")
            urllib.request.urlretrieve(
                BASE_URL + filename, filename, reporthook=download_progress
            )
            print()
        else:
            print(f"Using existing {filename}")


def build_shows_database():
    download_files()

    print("\nConnecting to DuckDB engine...")
    conn_duck = duckdb.connect()
    conn_duck.execute("SET enable_progress_bar = true;")

    print("\nStep 1/2: Extracting show-level metadata & cast with characters...")
    shows_query = """
    WITH all_basics AS (
        SELECT
            tconst,
            titleType,
            primaryTitle,
            originalTitle,
            TRY_CAST(startYear AS INTEGER) AS start_year,
            TRY_CAST(endYear AS INTEGER) AS end_year,
            TRY_CAST(runtimeMinutes AS INTEGER) AS runtime_minutes,
            genres
        FROM read_csv('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
        WHERE titleType IN ('tvSeries', 'tvMiniSeries', 'tvEpisode')
    ),
    show_basics AS (
        SELECT
            tconst AS show_id,
            primaryTitle AS title,
            originalTitle AS original_title,
            start_year,
            end_year,
            genres
        FROM all_basics
        WHERE titleType IN ('tvSeries', 'tvMiniSeries')
    ),
    episodes_raw AS (
        SELECT
            e.tconst AS episode_id,
            e.parentTconst AS show_id,
            COALESCE(TRY_CAST(e.seasonNumber AS INTEGER), 0) AS season_number
        FROM read_csv('title.episode.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) e
        WHERE e.parentTconst IN (SELECT show_id FROM show_basics)
    ),
    show_episode_stats AS (
        SELECT
            e.show_id,
            COUNT(DISTINCT NULLIF(e.season_number, 0)) AS total_seasons,
            COUNT(e.episode_id) AS total_episodes,
            SUM(b.runtime_minutes) AS total_time_taken_for_all_episodes
        FROM episodes_raw e
        LEFT JOIN all_basics b ON e.episode_id = b.tconst
        GROUP BY e.show_id
    ),
    show_ratings AS (
        SELECT
            tconst AS show_id,
            TRY_CAST(averageRating AS FLOAT) AS rating,
            TRY_CAST(numVotes AS INTEGER) AS vote_count
        FROM read_csv('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
    ),
    principals AS (
        SELECT
            p.tconst AS show_id,
            TRY_CAST(p.ordering AS INTEGER) AS ordering,
            p.nconst,
            p.category,
            p.job,
            p.characters,
            n.primaryName AS name
        FROM read_csv('title.principals.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) p
        JOIN read_csv('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) n
            ON p.nconst = n.nconst
        WHERE p.tconst IN (SELECT show_id FROM show_basics)
    ),
    distinct_principals AS (
        SELECT
            show_id,
            ordering,
            nconst,
            name,
            category,
            job,
            characters,
            ROW_NUMBER() OVER (
                PARTITION BY show_id, category, nconst
                ORDER BY ordering
            ) AS dup_rank
        FROM principals
    ),
    ranked_principals AS (
        SELECT
            show_id,
            ordering,
            nconst,
            name,
            category,
            job,
            CASE
                WHEN characters IS NOT NULL AND characters != '' AND characters != '[]' THEN
                    name || ' (as ' || replace(replace(replace(replace(characters, '["', ''), '"]', ''), '","', ', '), '", "', ', ') || ')'
                ELSE name
            END AS actor_display,
            ROW_NUMBER() OVER (
                PARTITION BY show_id, (category IN ('actor', 'actress'))
                ORDER BY ordering
            ) AS cast_rank
        FROM distinct_principals
        WHERE dup_rank = 1
    ),
    crew_agg AS (
        SELECT
            show_id,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'writer' OR LOWER(job) LIKE '%creator%') AS creators,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'writer' OR LOWER(job) LIKE '%creator%') AS creator_ids,
            string_agg(actor_display, ', ' ORDER BY ordering) FILTER (WHERE category IN ('actor', 'actress') AND cast_rank <= 6) AS casts,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category IN ('actor', 'actress') AND cast_rank <= 6) AS casts_id,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category NOT IN ('actor', 'actress')) AS crews,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category NOT IN ('actor', 'actress')) AS crews_id
        FROM ranked_principals
        GROUP BY show_id
    )
    SELECT
        b.show_id,
        b.title,
        b.original_title,
        b.start_year,
        b.end_year,
        s.total_seasons,
        s.total_episodes,
        s.total_time_taken_for_all_episodes,
        r.rating,
        r.vote_count,
        b.genres,
        c.creators,
        c.creator_ids,
        c.casts,
        c.casts_id,
        c.crews,
        c.crews_id
    FROM show_basics b
    LEFT JOIN show_episode_stats s ON b.show_id = s.show_id
    LEFT JOIN show_ratings r ON b.show_id = r.show_id
    LEFT JOIN crew_agg c ON b.show_id = c.show_id
    """
    shows_data = conn_duck.execute(shows_query).fetchall()
    print(f"Extracted {len(shows_data):,} TV shows.")

    print(
        "\nStep 2/2: Extracting season-level aggregations (including Season 0 Specials)..."
    )
    seasons_query = """
    WITH show_ids AS (
        SELECT tconst AS show_id
        FROM read_csv('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
        WHERE titleType IN ('tvSeries', 'tvMiniSeries')
    ),
    episodes AS (
        SELECT
            e.tconst AS episode_id,
            e.parentTconst AS show_id,
            COALESCE(TRY_CAST(e.seasonNumber AS INTEGER), 0) AS season_number
        FROM read_csv('title.episode.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) e
        WHERE e.parentTconst IN (SELECT show_id FROM show_ids)
    ),
    ratings AS (
        SELECT
            tconst AS episode_id,
            TRY_CAST(averageRating AS FLOAT) AS rating,
            TRY_CAST(numVotes AS INTEGER) AS vote_count
        FROM read_csv('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
    )
    SELECT
        e.show_id,
        e.season_number,
        COUNT(e.episode_id) AS season_total_episodes,
        ROUND(AVG(r.rating), 2) AS season_rating,
        SUM(r.vote_count) AS season_vote_count
    FROM episodes e
    LEFT JOIN ratings r ON e.episode_id = r.episode_id
    GROUP BY e.show_id, e.season_number
    ORDER BY e.show_id, e.season_number
    """
    seasons_data = conn_duck.execute(seasons_query).fetchall()
    print(f"Extracted {len(seasons_data):,} season records.")

    print("\nWriting records into SQLite database (shows.db)...")
    if os.path.exists("shows.db"):
        os.remove("shows.db")

    conn_sqlite = sqlite3.connect("shows.db")
    cursor = conn_sqlite.cursor()

    cursor.execute(
        """
        CREATE TABLE shows (
            show_id TEXT PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            start_year INTEGER,
            end_year INTEGER,
            total_seasons INTEGER,
            total_episodes INTEGER,
            total_time_taken_for_all_episodes INTEGER,
            rating REAL,
            vote_count INTEGER,
            genres TEXT,
            creators TEXT,
            creator_ids TEXT,
            casts TEXT,
            casts_id TEXT,
            crews TEXT,
            crews_id TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE seasons (
            show_id TEXT,
            season_number INTEGER,
            season_total_episodes INTEGER,
            season_rating REAL,
            season_vote_count INTEGER,
            PRIMARY KEY (show_id, season_number),
            FOREIGN KEY (show_id) REFERENCES shows (show_id)
        )
    """
    )

    cursor.executemany(
        "INSERT INTO shows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        shows_data,
    )
    cursor.executemany("INSERT INTO seasons VALUES (?, ?, ?, ?, ?)", seasons_data)

    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_shows_title ON shows(title)")
    cursor.execute("CREATE INDEX idx_shows_original_title ON shows(original_title)")
    cursor.execute("CREATE INDEX idx_shows_start_year ON shows(start_year)")
    cursor.execute("CREATE INDEX idx_shows_rating ON shows(rating)")
    cursor.execute("CREATE INDEX idx_seasons_show_id ON seasons(show_id)")

    conn_sqlite.commit()
    conn_sqlite.close()
    conn_duck.close()

    print("\nFinished! Database ready at shows.db")


if __name__ == "__main__":
    build_shows_database()
