import os
import sqlite3
import urllib.request
import duckdb

FILES = [
    "title.basics.tsv.gz",
    "title.ratings.tsv.gz",
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


def build_movies_db():
    download_files()

    print("\nConnecting to DuckDB engine...")
    conn_duck = duckdb.connect()
    conn_duck.execute("SET enable_progress_bar = true;")

    query = """
    WITH movie_basics AS (
        SELECT
            tconst,
            primaryTitle AS title,
            originalTitle AS original_title,
            TRY_CAST(startYear AS INTEGER) AS year,
            TRY_CAST(runtimeMinutes AS INTEGER) AS runtime_minutes,
            genres
        FROM read_csv('title.basics.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
        WHERE titleType = 'movie'
    ),
    ratings AS (
        SELECT
            tconst,
            TRY_CAST(averageRating AS FLOAT) AS rating,
            TRY_CAST(numVotes AS INTEGER) AS vote_count
        FROM read_csv('title.ratings.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True)
    ),
    principals AS (
        SELECT
            p.tconst,
            TRY_CAST(p.ordering AS INTEGER) AS ordering,
            p.nconst,
            p.category,
            p.characters,
            n.primaryName AS name
        FROM read_csv('title.principals.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) p
        JOIN read_csv('name.basics.tsv.gz', delim='\t', nullstr='\\N', quote='', header=True, all_varchar=True) n
            ON p.nconst = n.nconst
        WHERE p.tconst IN (SELECT tconst FROM movie_basics)
          AND p.category IN ('actor', 'actress', 'director', 'writer', 'producer', 'composer', 'cinematographer', 'editor')
    ),
    distinct_principals AS (
        SELECT
            tconst,
            ordering,
            nconst,
            name,
            category,
            characters,
            ROW_NUMBER() OVER (
                PARTITION BY tconst, category, nconst
                ORDER BY ordering
            ) AS dup_rank
        FROM principals
    ),
    ranked_principals AS (
        SELECT
            tconst,
            ordering,
            nconst,
            name,
            category,
            CASE
                WHEN characters IS NOT NULL AND characters != '' AND characters != '[]' THEN
                    name || ' (as ' || replace(replace(replace(replace(characters, '["', ''), '"]', ''), '","', ', '), '", "', ', ') || ')'
                ELSE name
            END AS actor_display,
            ROW_NUMBER() OVER (
                PARTITION BY tconst, (category IN ('actor', 'actress'))
                ORDER BY ordering
            ) AS cast_rank
        FROM distinct_principals
        WHERE dup_rank = 1
    ),
    crew_agg AS (
        SELECT
            tconst,
            string_agg(actor_display, ', ' ORDER BY ordering) FILTER (WHERE category IN ('actor', 'actress') AND cast_rank <= 6) AS cast,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category IN ('actor', 'actress') AND cast_rank <= 6) AS cast_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'director') AS directors,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'director') AS director_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'writer') AS writers,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'writer') AS writer_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'producer') AS producers,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'producer') AS producer_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'composer') AS composers,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'composer') AS composer_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'cinematographer') AS cinematographers,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'cinematographer') AS cinematographer_ids,
            string_agg(name, ', ' ORDER BY ordering) FILTER (WHERE category = 'editor') AS editors,
            string_agg(nconst, ', ' ORDER BY ordering) FILTER (WHERE category = 'editor') AS editor_ids
        FROM ranked_principals
        GROUP BY tconst
    )
    SELECT
        b.tconst,
        b.title,
        b.original_title,
        b.year,
        b.runtime_minutes,
        b.genres,
        r.rating,
        r.vote_count,
        c.cast,
        c.cast_ids,
        c.directors,
        c.director_ids,
        c.writers,
        c.writer_ids,
        c.producers,
        c.producer_ids,
        c.composers,
        c.composer_ids,
        c.cinematographers,
        c.cinematographer_ids,
        c.editors,
        c.editor_ids
    FROM movie_basics b
    LEFT JOIN ratings r ON b.tconst = r.tconst
    LEFT JOIN crew_agg c ON b.tconst = c.tconst
    """

    print("Executing query directly on compressed files...")
    results = conn_duck.execute(query).fetchall()
    print(f"\nExtracted {len(results):,} movies.")

    print("Writing records to movies.db...")
    if os.path.exists("movies.db"):
        os.remove("movies.db")

    conn_sqlite = sqlite3.connect("movies.db")
    cursor = conn_sqlite.cursor()

    cursor.execute(
        """
        CREATE TABLE movies (
            tconst TEXT PRIMARY KEY,
            title TEXT,
            original_title TEXT,
            year INTEGER,
            runtime_minutes INTEGER,
            genres TEXT,
            rating REAL,
            vote_count INTEGER,
            cast TEXT,
            cast_ids TEXT,
            directors TEXT,
            director_ids TEXT,
            writers TEXT,
            writer_ids TEXT,
            producers TEXT,
            producer_ids TEXT,
            composers TEXT,
            composer_ids TEXT,
            cinematographers TEXT,
            cinematographer_ids TEXT,
            editors TEXT,
            editor_ids TEXT
        )
    """
    )

    placeholders = ", ".join(["?"] * 22)
    cursor.executemany(f"INSERT INTO movies VALUES ({placeholders})", results)

    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_movies_title ON movies(title)")
    cursor.execute("CREATE INDEX idx_movies_original_title ON movies(original_title)")
    cursor.execute("CREATE INDEX idx_movies_year ON movies(year)")
    cursor.execute("CREATE INDEX idx_movies_rating ON movies(rating)")
    cursor.execute("CREATE INDEX idx_movies_vote_count ON movies(vote_count)")

    conn_sqlite.commit()
    conn_sqlite.close()
    conn_duck.close()
    print("Done! Database ready at movies.db")


if __name__ == "__main__":
    build_movies_db()
