import csv
import gzip
import json
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


def download_files():
    for filename in FILES:
        if not os.path.exists(filename):
            print(f"Downloading {filename}...")
            urllib.request.urlretrieve(BASE_URL + filename, filename)
        else:
            print(f"Using existing {filename}")


def build_movies_db():
    download_files()

    print("Step 1/5: Loading movie basics...")
    movies = {}
    with gzip.open("title.basics.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            if row["titleType"] == "movie":
                tconst = row["tconst"]
                movies[tconst] = {
                    "tconst": tconst,
                    "title": row["primaryTitle"],
                    "original_title": row["originalTitle"],
                    "year": int(row["startYear"]) if row["startYear"] != "\\N" else None,
                    "runtime_minutes": int(row["runtimeMinutes"]) if row["runtimeMinutes"] != "\\N" else None,
                    "genres": row["genres"] if row["genres"] != "\\N" else None,
                    "rating": None,
                    "vote_count": None,
                    "cast": [],
                    "directors": [],
                    "writers": [],
                }

    print("Step 2/5: Loading ratings...")
    with gzip.open("title.ratings.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            tconst = row["tconst"]
            if tconst in movies:
                movies[tconst]["rating"] = float(row["averageRating"])
                movies[tconst]["vote_count"] = int(row["numVotes"])

    print("Step 3/5: Extracting cast and crew IDs...")
    needed_names = set()
    with gzip.open("title.principals.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            tconst = row["tconst"]
            if tconst not in movies:
                continue

            nconst = row["nconst"]
            category = row["category"]
            chars = row["characters"]

            if category in ("actor", "actress") and len(movies[tconst]["cast"]) < 8:
                char_label = ""
                if chars != "\\N":
                    try:
                        parsed = json.loads(chars)
                        if parsed:
                            char_label = f" as {', '.join(parsed)}"
                    except Exception:
                        pass
                movies[tconst]["cast"].append((nconst, char_label))
                needed_names.add(nconst)

            elif category == "director":
                movies[tconst]["directors"].append(nconst)
                needed_names.add(nconst)

            elif category == "writer":
                movies[tconst]["writers"].append(nconst)
                needed_names.add(nconst)

    print("Step 4/5: Resolving person names...")
    names = {}
    with gzip.open("name.basics.tsv.gz", "rt", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t", quoting=csv.QUOTE_NONE)
        for row in reader:
            nconst = row["nconst"]
            if nconst in needed_names:
                names[nconst] = row["primaryName"]

    print("Step 5/5: Writing to SQLite database (movies.db)...")
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

    rows_to_insert = []
    for item in movies.values():
        cast_str = ", ".join([f"{names.get(n, n)}{c}" for n, c in item["cast"]])
        directors_str = ", ".join([names.get(n, n) for n in item["directors"]])
        writers_str = ", ".join([names.get(n, n) for n in item["writers"]])

        rows_to_insert.append(
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

    cursor.executemany(
        """
        INSERT INTO movies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
        rows_to_insert,
    )

    cursor.execute("CREATE INDEX idx_year ON movies(year)")
    cursor.execute("CREATE INDEX idx_rating ON movies(rating)")
    cursor.execute("CREATE INDEX idx_vote_count ON movies(vote_count)")

    conn.commit()
    conn.close()
    print("Done! SQLite file created: movies.db (table: movies)")


if __name__ == "__main__":
    build_movies_db()