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

TMDB_API_KEY_SHOWS = os.environ.get("TMDB_API_KEY_SHOWS")
SHARD_INDEX = int(os.environ.get("SHARD_INDEX", 0))
SHARD_TOTAL = int(os.environ.get("SHARD_TOTAL", 1))

CONCURRENCY = 20
DB_FILE = f"shows_shard_{SHARD_INDEX}.duckdb"
SHOWS_CSV = f"shows_shard_{SHARD_INDEX}.csv.gz"
SEASONS_CSV = f"seasons_shard_{SHARD_INDEX}.csv.gz"

HEADERS = {
    "Authorization": f"Bearer {TMDB_API_KEY_SHOWS}",
    "Accept": "application/json",
}

pause_event = asyncio.Event()
pause_event.set()
pause_lock = asyncio.Lock()


def init_db(db_path=DB_FILE):
    conn = duckdb.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shows (
            tmdb_id BIGINT PRIMARY KEY,
            imdb_id VARCHAR,
            title VARCHAR,
            original_title VARCHAR,
            backdrop_path VARCHAR,
            poster_path VARCHAR,
            start_year INTEGER,
            end_year INTEGER,
            status VARCHAR,
            genres VARCHAR,
            description VARCHAR,
            showrunner_and_creator VARCHAR,
            showrunner_and_creator_id VARCHAR,
            showrunner_and_creator_image VARCHAR,
            casts VARCHAR,
            casts_id VARCHAR,
            casts_image VARCHAR,
            crews VARCHAR,
            crews_id VARCHAR,
            crews_image VARCHAR,
            original_network VARCHAR,
            original_network_id VARCHAR,
            original_network_image VARCHAR,
            prod_studio VARCHAR,
            prod_studio_id VARCHAR,
            prod_studio_image VARCHAR,
            total_seasons INTEGER
        );

        CREATE TABLE IF NOT EXISTS seasons (
            show_tmdb_id BIGINT,
            show_imdb_id VARCHAR,
            season_number INTEGER,
            season_name VARCHAR,
            air_year INTEGER,
            poster_path VARCHAR,
            tmdb_episode_count INTEGER,
            PRIMARY KEY (show_tmdb_id, season_number)
        );
    """
    )
    conn.close()


async def fetch_show(session, semaphore, show_id, retries=5):
    url = f"https://api.themoviedb.org/3/tv/{show_id}?append_to_response=credits,external_ids"

    for attempt in range(retries):
        await pause_event.wait()

        async with semaphore:
            try:
                async with session.get(url, headers=HEADERS) as resp:
                    if resp.status == 200:
                        data = orjson.loads(await resp.read())

                        if data.get("adult", False):
                            return "ADULT", None, [], None, None

                        imdb_id = data.get("external_ids", {}).get("imdb_id")
                        if not imdb_id or not str(imdb_id).strip():
                            return "NO_IMDB", None, [], data.get("name"), None

                        credits = data.get("credits", {})
                        cast_list = credits.get("cast", [])
                        crew_list = credits.get("crew", [])
                        created_by = data.get("created_by", [])
                        networks = data.get("networks", [])
                        prod_list = data.get("production_companies", [])

                        first_air = data.get("first_air_date") or ""
                        last_air = data.get("last_air_date") or ""
                        start_year = (
                            int(first_air[:4])
                            if len(first_air) >= 4 and first_air[:4].isdigit()
                            else None
                        )
                        end_year = (
                            int(last_air[:4])
                            if len(last_air) >= 4 and last_air[:4].isdigit()
                            else None
                        )

                        show_record = (
                            data.get("id"),
                            imdb_id.strip(),
                            data.get("name"),
                            data.get("original_name"),
                            data.get("backdrop_path"),
                            data.get("poster_path"),
                            start_year,
                            end_year,
                            data.get("status"),
                            orjson.dumps(
                                [
                                    g.get("name")
                                    for g in data.get("genres", [])
                                    if g.get("name")
                                ]
                            ).decode("utf-8"),
                            data.get("overview"),
                            orjson.dumps([c.get("name") for c in created_by]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([c.get("id") for c in created_by]).decode(
                                "utf-8"
                            ),
                            orjson.dumps(
                                [c.get("profile_path") for c in created_by]
                            ).decode("utf-8"),
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
                            orjson.dumps([n.get("name") for n in networks]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([n.get("id") for n in networks]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([n.get("logo_path") for n in networks]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([p.get("name") for p in prod_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps([p.get("id") for p in prod_list]).decode(
                                "utf-8"
                            ),
                            orjson.dumps(
                                [p.get("logo_path") for p in prod_list]
                            ).decode("utf-8"),
                            data.get("number_of_seasons"),
                        )

                        seasons_records = []
                        for s in data.get("seasons", []):
                            s_air = s.get("air_date") or ""
                            s_year = (
                                int(s_air[:4])
                                if len(s_air) >= 4 and s_air[:4].isdigit()
                                else None
                            )
                            seasons_records.append(
                                (
                                    data.get("id"),
                                    imdb_id.strip(),
                                    s.get("season_number"),
                                    s.get("name"),
                                    s_year,
                                    s.get("poster_path"),
                                    s.get("episode_count"),
                                )
                            )

                        return (
                            "OK",
                            show_record,
                            seasons_records,
                            data.get("name"),
                            start_year,
                        )

                    elif resp.status == 404:
                        return "404", None, [], None, None

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

    return "ERR", None, [], None, None


async def writer_worker(queue, db_path=DB_FILE):
    conn = duckdb.connect(db_path)
    shows_batch = []
    seasons_batch = []

    while True:
        item = await queue.get()
        if item is None:
            break
        show_rec, seasons_recs = item
        shows_batch.append(show_rec)
        seasons_batch.extend(seasons_recs)

        if len(shows_batch) >= 200:
            conn.executemany(
                "INSERT OR REPLACE INTO shows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                shows_batch,
            )
            if seasons_batch:
                conn.executemany(
                    "INSERT OR REPLACE INTO seasons VALUES (?,?,?,?,?,?,?)",
                    seasons_batch,
                )
            shows_batch.clear()
            seasons_batch.clear()

    if shows_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO shows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            shows_batch,
        )
    if seasons_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO seasons VALUES (?,?,?,?,?,?,?)", seasons_batch
        )
    conn.close()


# ==========================================
# 1. FULL BASELINE SCRAPE
# ==========================================
async def download_tmdb_tv_dump(session):
    now = datetime.datetime.now(datetime.timezone.utc)
    candidate_dates = [
        now.strftime("%m_%d_%Y"),
        (now - datetime.timedelta(days=1)).strftime("%m_%d_%Y"),
    ]

    dump_filename = f"tv_series_ids_{SHARD_INDEX}.json.gz"
    downloaded = False

    for date_str in candidate_dates:
        url = f"http://files.tmdb.org/p/exports/tv_series_ids_{date_str}.json.gz"
        print(f"[*] [Shard {SHARD_INDEX}] Checking dump: {url}", flush=True)
        async with session.get(url) as resp:
            if resp.status == 200:
                with open(dump_filename, "wb") as f:
                    f.write(await resp.read())
                print(
                    f"[+] [Shard {SHARD_INDEX}] Downloaded TV dump for {date_str}",
                    flush=True,
                )
                downloaded = True
                break

    if not downloaded:
        raise RuntimeError("Failed to download TMDB daily TV dump.")

    query = f"SELECT id FROM read_json('{dump_filename}')"
    rows = duckdb.sql(query).fetchall()
    all_tv_ids = [r[0] for r in rows]

    if os.path.exists(dump_filename):
        os.remove(dump_filename)

    tv_ids = [
        t_id for idx, t_id in enumerate(all_tv_ids) if idx % SHARD_TOTAL == SHARD_INDEX
    ]
    print(
        f"[*] Total shows: {len(all_tv_ids):,} | Assigned to Shard {SHARD_INDEX}/{SHARD_TOTAL}: {len(tv_ids):,}",
        flush=True,
    )
    return tv_ids


async def run_full_scraper():
    init_db()

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=1500)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tv_ids = await download_tmdb_tv_dump(session)
        writer_task = asyncio.create_task(writer_worker(queue))

        processed = 0
        success_count = 0
        total = len(tv_ids)
        start_time = time.time()

        async def worker(t_id):
            nonlocal processed, success_count
            status, show_rec, seasons_recs, title, year = await fetch_show(
                session, semaphore, t_id
            )
            if status == "OK":
                success_count += 1
                await queue.put((show_rec, seasons_recs))

            processed += 1
            if processed % 50 == 0 or processed == total:
                elapsed = max(1, time.time() - start_time)
                speed = processed / elapsed
                pct = (processed / total) * 100
                print(
                    f"[Shard {SHARD_INDEX}/{SHARD_TOTAL}] Saved: {success_count:,} | Progress: {processed:,}/{total:,} ({pct:4.1f}%) | {speed:4.1f} req/s",
                    flush=True,
                )

        await asyncio.gather(*(worker(t_id) for t_id in tv_ids))
        await queue.put(None)
        await writer_task

    conn = duckdb.connect(DB_FILE)
    conn.execute(
        f"COPY shows TO '{SHOWS_CSV}' (HEADER, DELIMITER ',', COMPRESSION 'gzip');"
    )
    conn.execute(
        f"COPY seasons TO '{SEASONS_CSV}' (HEADER, DELIMITER ',', COMPRESSION 'gzip');"
    )
    conn.close()
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
    print(f"[*] Shard {SHARD_INDEX} completed.", flush=True)


# ==========================================
# 2. INCREMENTAL CHANGES (FAST DELTA)
# ==========================================
async def get_changed_tv_ids(session, start_date, end_date):
    page = 1
    total_pages = 1
    changed_ids = set()

    print(
        f"[*] Fetching TMDb TV changes from {start_date} to {end_date}...", flush=True
    )
    while page <= total_pages:
        url = f"https://api.themoviedb.org/3/tv/changes?start_date={start_date}&end_date={end_date}&page={page}"
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

    print(f"[+] Found {len(changed_ids):,} modified/new TV IDs.", flush=True)
    return list(changed_ids)


async def run_incremental(since_date):
    today = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")
    delta_db = "shows_delta.duckdb"
    init_db(delta_db)

    timeout = aiohttp.ClientTimeout(total=25)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY, keepalive_timeout=60)
    semaphore = asyncio.Semaphore(CONCURRENCY)
    queue = asyncio.Queue(maxsize=1000)

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tv_ids = await get_changed_tv_ids(session, since_date, today)
        if not tv_ids:
            print("[*] No changes detected.", flush=True)
            return

        writer_task = asyncio.create_task(writer_worker(queue, delta_db))
        processed = 0
        success_count = 0
        total = len(tv_ids)
        start_time = time.time()

        async def worker(t_id):
            nonlocal processed, success_count
            status, show_rec, seasons_recs, title, year = await fetch_show(
                session, semaphore, t_id
            )
            if status == "OK":
                success_count += 1
                await queue.put((show_rec, seasons_recs))
            processed += 1
            if processed % 50 == 0 or processed == total:
                elapsed = max(1, time.time() - start_time)
                speed = processed / elapsed
                print(
                    f"[Incremental] Saved: {success_count:,} | Progress: {processed:,}/{total:,} | {speed:4.1f} req/s",
                    flush=True,
                )

        await asyncio.gather(*(worker(t_id) for t_id in tv_ids))
        await queue.put(None)
        await writer_task

    print("[*] Merging incremental changes into master shows dataset...", flush=True)
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE master_shows AS SELECT * FROM 'shows_master.csv.gz';
        CREATE TABLE master_seasons AS SELECT * FROM 'seasons_master.csv.gz';
        ATTACH 'shows_delta.duckdb' AS delta_db;

        DELETE FROM master_shows WHERE tmdb_id IN (SELECT tmdb_id FROM delta_db.shows);
        INSERT INTO master_shows SELECT * FROM delta_db.shows;

        DELETE FROM master_seasons WHERE show_tmdb_id IN (SELECT show_tmdb_id FROM delta_db.seasons);
        INSERT INTO master_seasons SELECT * FROM delta_db.seasons;

        COPY master_shows TO 'shows_master.csv.gz' (HEADER, DELIMITER ',', COMPRESSION 'gzip');
        COPY master_seasons TO 'seasons_master.csv.gz' (HEADER, DELIMITER ',', COMPRESSION 'gzip');
    """
    )
    conn.close()
    if os.path.exists(delta_db):
        os.remove(delta_db)
    print("[+] Incremental show update complete.", flush=True)


# ==========================================
# 3. MERGE & REFRESH IMDB RATINGS
# ==========================================
def run_merge(
    shows_pattern="show_shards/shows_shard_*.csv.gz",
    seasons_pattern="show_shards/seasons_shard_*.csv.gz",
):
    files = {
        "title.ratings.tsv.gz": "https://datasets.imdbws.com/title.ratings.tsv.gz",
        "title.episode.tsv.gz": "https://datasets.imdbws.com/title.episode.tsv.gz",
        "title.basics.tsv.gz": "https://datasets.imdbws.com/title.basics.tsv.gz",
    }

    for filename, url in files.items():
        if not os.path.exists(filename):
            print(f"[*] Downloading {filename}...", flush=True)
            urllib.request.urlretrieve(url, filename)
            print(f"[+] Downloaded {filename}.", flush=True)

    print("[*] Enriching Shows and Seasons with IMDb metrics...", flush=True)
    conn = duckdb.connect()
    query = f"""
        CREATE OR REPLACE VIEW ep_stats AS
        SELECT
            e.parentTconst,
            COUNT(e.tconst) AS total_episodes,
            SUM(TRY_CAST(b.runtimeMinutes AS INTEGER)) AS total_time_taken_minutes
        FROM read_csv('title.episode.tsv.gz', delim='\\t', nullstr='\\\\N') e
        LEFT JOIN read_csv('title.basics.tsv.gz', delim='\\t', nullstr='\\\\N') b ON e.tconst = b.tconst
        GROUP BY e.parentTconst;

        CREATE OR REPLACE VIEW season_ratings AS
        SELECT
            e.parentTconst,
            COALESCE(TRY_CAST(e.seasonNumber AS INTEGER), 0) AS seasonNumber,
            ROUND(AVG(r.averageRating), 2) AS season_imdb_rating,
            SUM(r.numVotes) AS season_imdb_votes,
            COUNT(e.tconst) AS season_imdb_episodes
        FROM read_csv('title.episode.tsv.gz', delim='\\t', nullstr='\\\\N') e
        LEFT JOIN read_csv('title.ratings.tsv.gz', delim='\\t', nullstr='\\\\N') r ON e.tconst = r.tconst
        GROUP BY e.parentTconst, seasonNumber;

        COPY (
            SELECT
                s.*,
                r.averageRating AS imdb_rating,
                r.numVotes AS imdb_votes,
                ep.total_episodes AS imdb_total_episodes,
                ep.total_time_taken_minutes AS imdb_total_time_taken_minutes
            FROM read_csv_auto('{shows_pattern}') s
            LEFT JOIN read_csv('title.ratings.tsv.gz', delim='\\t', nullstr='\\\\N') r ON s.imdb_id = r.tconst
            LEFT JOIN ep_stats ep ON s.imdb_id = ep.parentTconst
        ) TO 'shows.csv.gz' (HEADER, COMPRESSION 'gzip');

        COPY (
            SELECT
                sn.*,
                sr.season_imdb_rating,
                sr.season_imdb_votes,
                sr.season_imdb_episodes
            FROM read_csv_auto('{seasons_pattern}') sn
            LEFT JOIN season_ratings sr ON sn.show_imdb_id = sr.parentTconst AND sn.season_number = sr.seasonNumber
        ) TO 'seasons.csv.gz' (HEADER, COMPRESSION 'gzip');
    """
    conn.execute(query)
    conn.close()
    print("[+] Successfully generated shows.csv.gz and seasons.csv.gz!", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "merge":
        s_pat = sys.argv[2] if len(sys.argv) > 2 else "show_shards/shows_shard_*.csv.gz"
        sn_pat = (
            sys.argv[3] if len(sys.argv) > 3 else "show_shards/seasons_shard_*.csv.gz"
        )
        run_merge(s_pat, sn_pat)
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
