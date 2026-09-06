import asyncio
import datetime
import os
import aiohttp
import duckdb
import orjson
import uvloop

# Install C-based libuv event loop for maximum network throughput
uvloop.install()

TMDB_API_KEY_MOVIES = os.environ.get("TMDB_API_KEY_MOVIES")
CONCURRENCY = 40
DB_FILE = "movies.duckdb"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY_MOVIES}",
    "Accept": "application/json",
}


def init_db():
    conn = duckdb.connect(DB_FILE)
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


async def download_tmdb_dump(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    candidate_dates = [
        now.strftime("%m_%d_%Y"),
        (now - datetime.timedelta(days=1)).strftime("%m_%d_%Y"),
    ]

    dump_filename = "movie_ids.json.gz"
    downloaded = False

    for date_str in candidate_dates:
        url = f"http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"
        print(f"[*] Checking dump: {url}")
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(dump_filename, "wb") as f:
                    f.write(await resp.read())
                print(f"[+] Downloaded dump for {date_str}")
                downloaded = True
                break

    if not downloaded:
        raise RuntimeError("Failed to download any valid TMDB daily dump.")

    print("[*] Parsing dump using DuckDB C++ engine...")
    # DuckDB reads and filters the gzipped JSON directly in ~1 second
    query = f"""
        SELECT id FROM read_json('{dump_filename}')
        WHERE adult = false
    """
    rows = duckdb.sql(query).fetchall()
    movie_ids = [r[0] for r in rows]

    # Clean up dump file
    if os.path.exists(dump_filename):
        os.remove(dump_filename)

    print(f"[*] Total non-adult movies to fetch: {len(movie_ids):,}")
    return movie_ids


async def fetch_movie(session, semaphore, movie_id, retries=5):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits"

    for attempt in range(retries):
        async with semaphore:
            try:
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status == 200:
                        # Fast Rust JSON parser
                        data = orjson.loads(await resp.read())
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

                        return (
                            data.get("id"),
                            data.get("imdb_id"),
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

                    elif resp.status == 404:
                        return None

                    elif resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 1.5))
                        await asyncio.sleep(retry_after + 0.1)
                        continue

                    else:
                        await asyncio.sleep(1 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(1 * (attempt + 1))

    return None


async def writer_worker(queue):
    conn = duckdb.connect(DB_FILE)
    batch = []

    while True:
        record = await queue.get()
        if record is None:
            break
        batch.append(record)
        if len(batch) >= 500:
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


async def export_csv():
    print("[*] DuckDB exporting table directly to compressed CSV...")
    conn = duckdb.connect(DB_FILE)
    # DuckDB can export directly to gzip-compressed CSV via SQL
    conn.execute(
        "COPY movies TO 'movies.csv.gz' (HEADER, DELIMITER ',', COMPRESSION 'gzip');"
    )
    conn.close()
    print("[*] Exported to movies.csv.gz")


async def main():
    init_db()

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=2000)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        movie_ids = await download_tmdb_dump(session)
        writer_task = asyncio.create_task(writer_worker(queue))

        processed = 0
        total = len(movie_ids)

        async def worker(m_id):
            nonlocal processed
            res = await fetch_movie(session, semaphore, m_id)
            if res:
                await queue.put(res)
            processed += 1
            if processed % 2000 == 0 or processed == total:
                print(
                    f"Progress: {processed:,} / {total:,} ({processed / total * 100:.1f}%)"
                )

        await asyncio.gather(*(worker(m_id) for m_id in movie_ids))

        await queue.put(None)
        await writer_task

    await export_csv()
    print("[*] Completed successfully.")


if __name__ == "__main__":
    asyncio.run(main())
