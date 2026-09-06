import asyncio
import datetime
import os
import sys
import time
import urllib.request
import aiohttp
import duckdb
import orjson
import uvloop

uvloop.install()

TMDB_API_KEY_MOVIES = os.environ.get("TMDB_API_KEY_MOVIES")
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", 0))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", 1))

CONCURRENCY = 20
DB_FILE = f"movies_shard_{SHARD_INDEX}.duckdb"
CSV_FILE = f"movies_shard_{SHARD_INDEX}.csv.gz"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY_MOVIES}",
    "Accept": "application/json",
}

pause_event = asyncio.Event()
pause_event.set()
pause_lock = asyncio.Lock()


def init_db(db_path=DB_FILE):
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            tmdb_id BIGINT PRIMARY KEY,
            imdb_id VARCHAR,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            year INTEGER,
            runtime INTEGER,
            title VARCHAR,
            original_title VARCHAR,
            status VARCHAR,
            genres VARCHAR,
            description VARCHAR,
            casts VARCHAR,
            casts_id VARCHAR,
            casts_image_path VARCHAR,
            crews VARCHAR,
            crews_id VARCHAR,
            crews_image_path VARCHAR,
            prod_studio VARCHAR,
            prod_studio_id VARCHAR,
            prod_studio_image_path VARCHAR
        )
    """
    )
    conn.close()


async def fetch_movie(session, semaphore, movie_id, retries=5):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits"

    for attempt in range(retries):
        await pause_event.wait()

        async with semaphore:
            try:
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status == 200:
                        data = orjson.loads(await resp.read())

                        if data.get("adult", False):
                            return "ADULT", None, None, None

                        imdb_id = data.get("imdb_id")
                        if not imdb_id or not str(imdb_id).strip():
                            return "NO_IMDB", None, data.get("title"), None

                        credits = data.get("credits", {})
                        cast_list = credits.get("cast", [])
                        crew_list = credits.get("crew", [])
                        prod_list = data.get("production_companies", [])

                        release_date = data.get("release_date") or ""
                        year = (
                            int(release_date[:4])
                            if len(release_date) >= 4 and release_date[:4].isdigit()
                            else None
                        )

                        record = (
                            data.get("id"),
                            imdb_id.strip(),
                            data.get("backdrop_path"),
                            data.get("poster_path"),
                            year,
                            data.get("runtime"),
                            data.get("title"),
                            data.get("original_title"),
                            data.get("status"),
                            orjson.dumps(
                                [
                                    g.get("name")
                                    for g in data.get("genres", [])
                                    if g.get("name")
                                ]
                            ).decode("utf-8"),
                            data.get("overview"),
                            orjson.dumps([c.get("name") for c in cast_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([c.get("id") for c in cast_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps(
                                [c.get("profile_path") for c in cast_list]
                            ).decode("utf-8"),
                            orjson.dumps([c.get("name") for c in crew_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([c.get("id") for c in crew_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps(
                                [c.get("profile_path") for c in crew_list]
                            ).decode("utf-8"),
                            orjson.dumps([p.get("name") for p in prod_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([p.get("id") for p in prod_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps(
                                [p.get("logo_path") for p in prod_list]
                            ).decode("utf-8"),
                        )
                        return "OK", record, data.get("title"), year

                    elif resp.status == 404:
                        return "404", None, None, None

                    elif resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 2.0))
                        async with pause_lock:
                            if pause_event.is_set():
                                pause_event.clear()
                                print(
                                    f"\n[429] Rate limit hit. Pausing for {retry_after:.1f}s:",
                                    flush=True,
                                )
                                remaining = retry_after
                                while remaining > 0:
                                    print(
                                        f"  [429] {remaining:.1f}s remaining...",
                                        flush=True,
                                    )
                                    step = min(1.0, remaining)
                                    await asyncio.sleep(step)
                                    remaining -= step
                                print("  [429] Resuming workers.\n", flush=True)
                                pause_event.set()

                        await pause_event.wait()
                        continue

                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(0.5 * (attempt + 1))

    return "ERR", None, None, None


async def writer_worker(queue, db_path=DB_FILE):
    conn = duckdb.connect(db_path)
    batch = []

    while True:
        record = await queue.get()
        if record is None:
            break
        batch.append(record)
        if len(batch) >= 200:
            conn.executemany(
                "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
            batch.clear()

    if batch:
        conn.executemany(
            "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch,
        )
    conn.close()


# ==========================================
# 1. FULL BASELINE SCRAPE
# ==========================================
async def download_tmdb_dump(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    candidate_dates = [
        now.strftime("%m_%d_%Y"),
        (now - datetime.timedelta(days=1)).strftime("%m_%d_%Y"),
    ]

    dump_filename = f"movie_ids_{SHARD_INDEX}.json.gz"
    downloaded = False

    for date_str in candidate_dates:
        url = f"http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"
        print(f"[*] [Shard {SHARD_INDEX}] Checking dump: {url}", flush=True)
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(dump_filename, "wb") as f:
                    f.write(await resp.read())
                print(
                    f"[+] [Shard {SHARD_INDEX}] Downloaded dump for {date_str}",
                    flush=True,
                )
                downloaded = True
                break

    if not downloaded:
        raise RuntimeError("Failed to download TMDB daily dump.")

    query = f"SELECT id FROM read_json('{dump_filename}') WHERE adult = false"
    rows = duckdb.sql(query).fetchall()
    all_movie_ids = [r[0] for r in rows]

    if os.path.exists(dump_filename):
        os.remove(dump_filename)

    movie_ids = [
        m_id
        for idx, m_id in enumerate(all_movie_ids)
        if idx % SHARD_TOTAL == SHARD_INDEX
    ]
    print(
        f"[*] Total non-adult movies: {len(all_movie_ids):,} | Assigned to Shard {SHARD_INDEX}/{SHARD_TOTAL}: {len(movie_ids):,}",
        flush=True,
    )
    return movie_ids


async def run_full_scraper():
    init_db()

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=1500)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        movie_ids = await download_tmdb_dump(session)
        writer_task = asyncio.create_task(writer_worker(queue))

        processed = 0
        success_count = 0
        total = len(movie_ids)
        start_time = time.time()

        async def worker(m_id):
            nonlocal processed, success_count
            status, rec, title, year = await fetch_movie(session, semaphore, m_id)

            if status == "OK":
                success_count += 1
                await queue.put(rec)

            processed += 1
            if processed % 50 == 0 or processed == total:
                elapsed = max(1, time.time() - start_time)
                speed = processed / elapsed
                pct = (processed / total) * 100
                print(
                    f"[Shard {SHARD_INDEX}/{SHARD_TOTAL}] Saved: {success_count:,} | Progress: {processed:,}/{total:,} ({pct:4.1f}%) | {speed:4.1f} req/s",
                    flush=True,
                )

        await asyncio.gather(*(worker(m_id) for m_id in movie_ids))
        await queue.put(None)
        await writer_task

    conn = duckdb.connect(DB_FILE)
    conn.execute(
        f"COPY movies TO '{CSV_FILE}' (HEADER, DELIMITER ',', COMPRESSION 'gzip');"
    )
    conn.close()
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    print(f"[*] Shard {SHARD_INDEX} completed.", flush=True)


# ==========================================
# 2. INCREMENTAL CHANGES (FAST DELTA)
# ==========================================
async def get_changed_movie_ids(session, start_date, end_date):
    page = 1
    total_pages = 1
    changed_ids = set()

    print(
        f"[*] Fetching TMDb movie changes from {start_date} to {end_date}...",
        flush=True,
    )
    while page <= total_pages:
        url = f"https://api.themoviedb.org/3/movie/changes?start_date={start_date}&end_date={end_date}&page={page}"
        async with session.get(url, headers=HEADERS) as resp:
            if resp.status == 200:
                data = orjson.loads(await resp.read())
                total_pages = data.get("total_pages", 1)
                for item in data.get("results", []):
                    if not item.get("adult", False):
                        changed_ids.add(item["id"])
                page += 1
            elif resp.status == 429:
                await asyncio.sleep(2)
                continue
            else:
                break

    print(f"[+] Found {len(changed_ids):,} modified/new movie IDs.", flush=True)
    return list(changed_ids)


async def run_incremental(since_date):
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    delta_db = "movies_delta.duckdb"
    init_db(delta_db)

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=1000)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        movie_ids = await get_changed_movie_ids(session, since_date, today)
        if not movie_ids:
            print("[*] No changes detected.", flush=True)
            return

        writer_task = asyncio.create_task(writer_worker(queue, delta_db))
        processed = 0
        success_count = 0
        total = len(movie_ids)
        start_time = time.time()

        async def worker(m_id):
            nonlocal processed, success_count
            status, rec, title, year = await fetch_movie(session, semaphore, m_id)
            if status == "OK":
                success_count += 1
                await queue.put(rec)
            processed += 1
            if processed % 50 == 0 or processed == total:
                elapsed = max(1, time.time() - start_time)
                speed = processed / elapsed
                print(
                    f"[Incremental] Saved: {success_count:,} | Progress: {processed:,}/{total:,} | {speed:4.1f} req/s",
                    flush=True,
                )

        await asyncio.gather(*(worker(m_id) for m_id in movie_ids))
        await queue.put(None)
        await writer_task

    print("[*] Merging incremental changes into master movies dataset...", flush=True)
    conn = duckdb.connect()
    # Upsert delta into master table
    conn.execute(
        """
        CREATE TABLE master_movies AS SELECT * FROM 'movies_master.csv.gz';
        ATTACH 'movies_delta.duckdb' AS delta_db;
        DELETE FROM master_movies WHERE tmdb_id IN (SELECT tmdb_id FROM delta_db.movies);
        INSERT INTO master_movies SELECT * FROM delta_db.movies;
        COPY master_movies TO 'movies_master.csv.gz' (HEADER, DELIMITER ',', COMPRESSION 'gzip');
    """
    )
    conn.close()
    if os.path.exists(delta_db):
        os.remove(delta_db)
    print("[+] Incremental movie update complete.", flush=True)


# ==========================================
# 3. MERGE & REFRESH IMDB RATINGS
# ==========================================
def run_merge(input_pattern="movie_shards/movies_shard_*.csv.gz"):
    ratings_url = "https://datasets.imdbws.com/title.ratings.tsv.gz"
    ratings_file = "title.ratings.tsv.gz"
    output_file = "movies.csv.gz"

    if not os.path.exists(ratings_file):
        print(f"[*] Downloading fresh {ratings_url}...", flush=True)
        urllib.request.urlretrieve(ratings_url, ratings_file)
        print("[+] Download complete.", flush=True)

    print("[*] Merging shards and refreshing IMDb ratings in DuckDB...", flush=True)
    conn = duckdb.connect()
    query = f"""
        COPY (
            SELECT
                m.*,
                r.averageRating AS imdb_rating,
                r.numVotes AS imdb_votes
            FROM read_csv_auto('{input_pattern}') m
            LEFT JOIN read_csv('{ratings_file}', delim='\\t', nullstr='\\\\N') r
                ON m.imdb_id = r.tconst
        ) TO '{output_file}' (HEADER, COMPRESSION 'gzip');
    """
    conn.execute(query)
    conn.close()
    print(f"[+] Successfully refreshed {output_file}!", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "merge":
        pattern = (
            sys.argv[2] if len(sys.argv) > 2 else "movie_shards/movies_shard_*.csv.gz"
        )
        run_merge(pattern)
    elif len(sys.argv) > 1 and sys.argv[1] == "incremental":
        since = (
            sys.argv[2]
            if len(sys.argv) > 2
            else (
                datetime.datetime.now(datetime.timezone.utc)
                - datetime.timedelta(days=2)
            ).strftime("%Y-%m-%d")
        )
        asyncio.run(run_incremental(since))
    else:
        asyncio.run(run_full_scraper())
