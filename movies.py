import asyncio
import datetime
import gzip
import json
import os
import sqlite3
import aiohttp

# Reads your v4 Read Access Token from GitHub Secrets / Environment
TMDB_API_KEY_MOVIES = os.environ.get("TMDB_API_KEY_MOVIES")

# Concurrency pool (35-40 keeps TMDB throughput high without instant drops)
CONCURRENCY = 40
DB_FILE = "movies.db"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY_MOVIES}",
    "Accept": "application/json",
}


def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            tmdb_id INTEGER PRIMARY KEY,
            imdb_id TEXT,
            backdrop_path TEXT,
            poster_path TEXT,
            year INTEGER,
            runtime INTEGER,
            title TEXT,
            original_title TEXT,
            status TEXT,
            genres TEXT,
            description TEXT,
            casts TEXT,
            casts_id TEXT,
            casts_image_path TEXT,
            crews TEXT,
            crews_id TEXT,
            crews_image_path TEXT,
            prod_studio TEXT,
            prod_studio_id TEXT,
            prod_studio_image_path TEXT
        )
    """
    )
    conn.commit()
    conn.close()


async def download_tmdb_dump(session):
    # Try today's date first, fallback to yesterday if export is not ready yet
    now = datetime.datetime.now(datetime.timezone.utc)
    candidate_dates = [
        now.strftime("%m_%d_%Y"),
        (now - datetime.timedelta(days=1)).strftime("%m_%d_%Y"),
    ]

    compressed_data = None
    for date_str in candidate_dates:
        url = f"http://files.tmdb.org/p/exports/movie_ids_{date_str}.json.gz"
        print(f"[*] Checking dump: {url}")
        async with session.get(url) as resp:
            if resp.status == 200:
                compressed_data = await resp.read()
                print(f"[+] Downloaded dump for {date_str}")
                break

    if not compressed_data:
        raise RuntimeError("Failed to download any valid TMDB daily dump.")

    decompressed = gzip.decompress(compressed_data).decode("utf-8")
    movie_ids = []

    for line in decompressed.strip().split("\n"):
        if not line:
            continue
        item = json.loads(line)
        # Only filter: ignore adult content
        if not item.get("adult", False):
            movie_ids.append(item["id"])

    print(f"[*] Total non-adult movies to fetch: {len(movie_ids):,}")
    return movie_ids


async def fetch_movie(session, semaphore, movie_id, retries=5):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?append_to_response=credits"

    for attempt in range(retries):
        async with semaphore:
            try:
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status == 200:
                        data = await resp.json()
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
                            json.dumps(
                                [
                                    g.get("name")
                                    for g in data.get("genres", [])
                                    if g.get("name")
                                ]
                            ),
                            data.get("overview"),
                            json.dumps([c.get("name") for c in cast_list]),
                            json.dumps([c.get("id") for c in cast_list]),
                            json.dumps([c.get("profile_path") for c in cast_list]),
                            json.dumps([c.get("name") for c in crew_list]),
                            json.dumps([c.get("id") for c in crew_list]),
                            json.dumps([c.get("profile_path") for c in crew_list]),
                            json.dumps([p.get("name") for p in prod_list]),
                            json.dumps([p.get("id") for p in prod_list]),
                            json.dumps([p.get("logo_path") for p in prod_list]),
                        )

                    elif resp.status == 404:
                        return None

                    elif resp.status == 429:
                        # Follow the Retry-After header provided by TMDB
                        retry_after = float(resp.headers.get("Retry-After", 1.5))
                        wait_time = retry_after + 0.1
                        await asyncio.sleep(wait_time)
                        continue

                    else:
                        await asyncio.sleep(1 * (attempt + 1))

            except (aiohttp.ClientError, asyncio.TimeoutError):
                await asyncio.sleep(1 * (attempt + 1))

    return None


async def writer_worker(queue):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    batch = []

    while True:
        record = await queue.get()
        if record is None:
            break
        batch.append(record)
        # Commit in batches of 250 to keep memory footprint minimal
        if len(batch) >= 250:
            c.executemany(
                "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                batch,
            )
            conn.commit()
            batch.clear()

    if batch:
        c.executemany(
            "INSERT OR REPLACE INTO movies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch,
        )
        conn.commit()
    conn.close()


async def main():
    init_db()

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=1000)

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

        # Stop database writer and finish
        await queue.put(None)
        await writer_task

    print("[*] Completed successfully. Data stored in movies.db")


if __name__ == "__main__":
    asyncio.run(main())
