import asyncio
import datetime
import os
import time
import aiohttp
import duckdb
import orjson
import uvloop

uvloop.install()

TMDB_API_KEY_MOVIES = os.environ.get("TMDB_API_KEY_MOVIES")
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", 0))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", 1))

CONCURRENCY = 40
DB_FILE = f"movies_shard_{SHARD_INDEX}.duckdb"
CSV_FILE = f"movies_shard_{SHARD_INDEX}.csv.gz"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY_MOVIES}",
    "Accept": "application/json",
}

# Global event to pause all workers together during a 429 cooldown
pause_event = asyncio.Event()
pause_event.set()
pause_lock = asyncio.Lock()


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
        raise RuntimeError("Failed to download any valid TMDB daily dump.")

    # DuckDB filters adult movies out in C++
    query = f"SELECT id FROM read_json('{dump_filename}') WHERE adult = false"
    rows = duckdb.sql(query).fetchall()
    all_movie_ids = [r[0] for r in rows]

    if os.path.exists(dump_filename):
        os.remove(dump_filename)

    # Shard slicing: partition across runners
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


async def fetch_movie(session, semaphore, movie_id, retries=5):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits"

    for attempt in range(retries):
        # Wait if another worker triggered a rate-limit cooldown
        await pause_event.wait()

        async with semaphore:
            try:
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status == 200:
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

                        record = (
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
                        return "OK", record, data.get("title"), year

                    elif resp.status == 404:
                        return "404", None, None, None

                    elif resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", 2.0))

                        # Lock so only one worker controls the countdown timer in the log
                        async with pause_lock:
                            if pause_event.is_set():
                                pause_event.clear()
                                print(
                                    f"\n[!] ⚠️ Rate limit (HTTP 429) hit! Pausing all workers for {retry_after:.1f}s:",
                                    flush=True,
                                )
                                remaining = retry_after
                                while remaining > 0:
                                    print(
                                        f"    ⏳ Waiting... {remaining:.1f}s remaining",
                                        flush=True,
                                    )
                                    step = min(1.0, remaining)
                                    await asyncio.sleep(step)
                                    remaining -= step
                                print(
                                    "    ✅ Cooldown complete! Resuming workers...\n",
                                    flush=True,
                                )
                                pause_event.set()

                        await pause_event.wait()
                        continue

                    else:
                        await asyncio.sleep(0.5 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(0.5 * (attempt + 1))

    return "ERR", None, None, None


async def writer_worker(queue):
    conn = duckdb.connect(DB_FILE)
    batch = []

    while True:
        record = await queue.get()
        if record is None:
            break
        batch.append(record)
        if len(batch) >= 250:
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
    print(
        f"[*] DuckDB exporting Shard {SHARD_INDEX} directly to {CSV_FILE}...",
        flush=True,
    )
    conn = duckdb.connect(DB_FILE)
    conn.execute(
        f"COPY movies TO '{CSV_FILE}' (HEADER, DELIMITER ',', COMPRESSION 'gzip');"
    )
    conn.close()
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    print(f"[+] Shard {SHARD_INDEX} exported successfully.", flush=True)


async def main():
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
        not_found_count = 0
        error_count = 0
        total = len(movie_ids)
        start_time = time.time()

        async def worker(m_id):
            nonlocal processed, success_count, not_found_count, error_count
            status, rec, title, year = await fetch_movie(session, semaphore, m_id)

            if status == "OK":
                success_count += 1
                await queue.put(rec)
            elif status == "404":
                not_found_count += 1
            else:
                error_count += 1

            processed += 1

            # Log updates every 50 items so the terminal moves continuously in real-time
            if processed % 50 == 0 or processed == total:
                elapsed = max(1, time.time() - start_time)
                speed = processed / elapsed
                pct = (processed / total) * 100
                display_title = (
                    (title[:25] + "...")
                    if title and len(title) > 25
                    else (title or "N/A")
                )
                print(
                    f"[{SHARD_INDEX}/{SHARD_TOTAL}] {processed:,}/{total:,} ({pct:5.1f}%) | "
                    f"{speed:4.1f} req/s | OK: {success_count:,} | 404: {not_found_count} | "
                    f"Q: {queue.qsize():3d} | '{display_title}' ({year or '?'})",
                    flush=True,
                )

        await asyncio.gather(*(worker(m_id) for m_id in movie_ids))

        await queue.put(None)
        await writer_task

    await export_csv()
    print(f"[*] Shard {SHARD_INDEX} finished complete run.", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
