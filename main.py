import csv
import gzip
import os
import sqlite3
import urllib.request

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

    print("\n[Step 1/5] Loading title.basics.tsv.gz...")
    movies = {}
    row_count = 0
    with gzip.open("title.basics.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            row_count += 1
            if row_count % 1_000_000 == 0:
                print(
                    f"\rRead {row_count:,} rows | Movies found: {len(movies):,}",
                    end="",
                    flush=True,
                )

            if row["titleType"] == "movie":
                tconst = row["tconst"]
                movies[tconst] = {
                    "tconst": tconst,
                    "title": row["primaryTitle"],
                    "original_title": row["originalTitle"],
                    "year": int(row["startYear"])
                    if row["startYear"] != "\\N"
                    else None,
                    "runtime_minutes": int(row["runtimeMinutes"])
                    if row["runtimeMinutes"] != "\\N"
                    else None,
                    "genres": row["genres"] if row["genres"] != "\\N" else None,
                    "rating": None,
                    "vote_count": None,
                    "cast": [],
                    "directors": [],
                    "writers": [],
                }
    print(
        f"\rFinished: Scanned {row_count:,} rows | Total movies kept: {len(movies):,}"
    )

    print("\n[Step 2/5] Loading title.ratings.tsv.gz...")
    row_count = 0
    with gzip.open("title.ratings.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            row_count += 1
            if row_count % 500_000 == 0:
                print(f"\rRead {row_count:,} ratings...", end="", flush=True)

            tconst = row["tconst"]
            if tconst in movies:
                movies[tconst]["rating"] = float(row["averageRating"])
                movies[tconst]["vote_count"] = int(row["numVotes"])
    print(f"\rFinished: Scanned {row_count:,} ratings.")

    print("\n[Step 3/5] Loading title.principals.tsv.gz (Cast & Crew)...")
    needed_names = set()
    row_count = 0
    with gzip.open("title.principals.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            row_count += 1
            if row_count % 2_000_000 == 0:
                print(
                    f"\rProcessed {row_count:,} / ~65,000,000 rows...",
                    end="",
                    flush=True,
                )

            tconst = row["tconst"]
            if tconst not in movies:
                continue

            nconst = row["nconst"]
            category = row["category"]

            if category in ("actor", "actress") and len(movies[tconst]["cast"]) < 6:
                movies[tconst]["cast"].append(nconst)
                needed_names.add(nconst)
            elif category == "director":
                movies[tconst]["directors"].append(nconst)
                needed_names.add(nconst)
            elif category == "writer":
                movies[tconst]["writers"].append(nconst)
                needed_names.add(nconst)
    print(
        f"\rFinished: Processed {row_count:,} rows | Unique people needed: {len(needed_names):,}"
    )

    print("\n[Step 4/5] Loading name.basics.tsv.gz...")
    names = {}
    row_count = 0
    with gzip.open("name.basics.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            row_count += 1
            if row_count % 2_000_000 == 0:
                print(
                    f"\rProcessed {row_count:,} / ~14,000,000 rows | Names resolved: {len(names):,}",
                    end="",
                    flush=True,
                )

            nconst = row["nconst"]
            if nconst in needed_names:
                names[nconst] = row["primaryName"]
    print(f"\rFinished: Resolved {len(names):,} names.")

    print("\n[Step 5/5] Building movies.db...")
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
            directors TEXT,
            writers TEXT
        )
    """
    )

    batch = []
    for item in movies.values():
        cast_str = ", ".join([names.get(n, n) for n in item["cast"]])
        directors_str = ", ".join([names.get(n, n) for n in item["directors"]])
        writers_str = ", ".join([names.get(n, n) for n in item["writers"]])

        batch.append(
            (
                item["tconst"],
                item["title"],
                item["original_title"],
                item["year"],
                item["runtime_minutes"],
                item["genres"],
                item["rating"],
                item["vote_count"],
                cast_str if cast_str else None,
                directors_str if directors_str else None,
                writers_str if writers_str else None,
            )
        )

    print("Inserting rows into database...")
    cursor.executemany(
        "INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", batch
    )

    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_year ON movies(year)")
    cursor.execute("CREATE INDEX idx_rating ON movies(rating)")
    cursor.execute("CREATE INDEX idx_vote_count ON movies(vote_count)")

    conn.commit()
    conn.close()
    print("\nAll done! You can now open and inspect movies.db")


if __name__ == "__main__":
    build_movies_db()
