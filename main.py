import os
import sqlite3
import urllib.request
import polars as pl

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

    print("\nProcessing datasets with Polars...")

    basics = (
        pl.scan_csv(
            "title.basics.tsv.gz",
            separator="\t",
            null_values=["\\N"],
            quote_char=None,
        )
        .filter(pl.col("titleType") == "movie")
        .select(
            [
                pl.col("tconst"),
                pl.col("primaryTitle").alias("title"),
                pl.col("originalTitle").alias("original_title"),
                pl.col("startYear").cast(pl.Int32, strict=False).alias("year"),
                pl.col("runtimeMinutes")
                .cast(pl.Int32, strict=False)
                .alias("runtime_minutes"),
                pl.col("genres"),
            ]
        )
    )

    ratings = pl.scan_csv(
        "title.ratings.tsv.gz",
        separator="\t",
        null_values=["\\N"],
        quote_char=None,
    ).select(
        [
            pl.col("tconst"),
            pl.col("averageRating").cast(pl.Float32, strict=False).alias("rating"),
            pl.col("numVotes").cast(pl.Int32, strict=False).alias("vote_count"),
        ]
    )

    names = pl.scan_csv(
        "name.basics.tsv.gz",
        separator="\t",
        null_values=["\\N"],
        quote_char=None,
    ).select(
        [
            pl.col("nconst"),
            pl.col("primaryName").alias("name"),
        ]
    )

    movie_principals = (
        pl.scan_csv(
            "title.principals.tsv.gz",
            separator="\t",
            null_values=["\\N"],
            quote_char=None,
        )
        .select(["tconst", "ordering", "nconst", "category"])
        .join(basics.select("tconst"), on="tconst", how="inner")
        .join(names, on="nconst", how="inner")
    )

    cast_df = (
        movie_principals.filter(pl.col("category").is_in(["actor", "actress"]))
        .sort("ordering")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").head(6).str.join(", ").alias("cast"),
                pl.col("nconst").head(6).str.join(", ").alias("cast_ids"),
            ]
        )
    )

    directors_df = (
        movie_principals.filter(pl.col("category") == "director")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("directors"),
                pl.col("nconst").str.join(", ").alias("director_ids"),
            ]
        )
    )

    writers_df = (
        movie_principals.filter(pl.col("category") == "writer")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("writers"),
                pl.col("nconst").str.join(", ").alias("writer_ids"),
            ]
        )
    )

    producers_df = (
        movie_principals.filter(pl.col("category") == "producer")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("producers"),
                pl.col("nconst").str.join(", ").alias("producer_ids"),
            ]
        )
    )

    composers_df = (
        movie_principals.filter(pl.col("category") == "composer")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("composers"),
                pl.col("nconst").str.join(", ").alias("composer_ids"),
            ]
        )
    )

    cinematographers_df = (
        movie_principals.filter(pl.col("category") == "cinematographer")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("cinematographers"),
                pl.col("nconst").str.join(", ").alias("cinematographer_ids"),
            ]
        )
    )

    editors_df = (
        movie_principals.filter(pl.col("category") == "editor")
        .group_by("tconst")
        .agg(
            [
                pl.col("name").str.join(", ").alias("editors"),
                pl.col("nconst").str.join(", ").alias("editor_ids"),
            ]
        )
    )

    print("Joining tables and executing computation graph...")
    final_df = (
        basics.join(ratings, on="tconst", how="left")
        .join(cast_df, on="tconst", how="left")
        .join(directors_df, on="tconst", how="left")
        .join(writers_df, on="tconst", how="left")
        .join(producers_df, on="tconst", how="left")
        .join(composers_df, on="tconst", how="left")
        .join(cinematographers_df, on="tconst", how="left")
        .join(editors_df, on="tconst", how="left")
        .collect()
    )

    print(f"Total movies processed: {final_df.height:,}")

    print("Writing records to movies.db...")
    if os.path.exists("movies.db"):
        os.remove("movies.db")

    conn = sqlite3.connect("movies.db")
    cursor = conn.cursor()

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

    placeholders = ", ".join(["?"] * len(final_df.columns))
    cursor.executemany(
        f"INSERT INTO movies VALUES ({placeholders})", final_df.iter_rows()
    )

    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_year ON movies(year)")
    cursor.execute("CREATE INDEX idx_rating ON movies(rating)")
    cursor.execute("CREATE INDEX idx_vote_count ON movies(vote_count)")

    conn.commit()
    conn.close()
    print("Done! Database ready at movies.db")


if __name__ == "__main__":
    build_movies_db()
