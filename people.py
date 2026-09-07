import os
import sqlite3
import time
import orjson

DB_PATH = "media.sqlite"


def normalize_people():
    if not os.path.exists(DB_PATH):
        raise FileNotFoundError(f"'{DB_PATH}' not found. Run build_master_db.py first.")

    start_time = time.time()
    print(
        "[*] Normalizing cast, crew, creators, and directors across all media...",
        flush=True,
    )

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Fast SQLite execution settings
    cur.execute("PRAGMA synchronous = OFF;")
    cur.execute("PRAGMA journal_mode = MEMORY;")
    cur.execute("PRAGMA cache_size = -64000;")  # 64MB memory cache

    # 1. Create normalized tables
    cur.execute("""
        CREATE TABLE IF NOT EXISTS people (
            id INTEGER PRIMARY KEY,
            tmdb_id INTEGER UNIQUE,
            name TEXT NOT NULL,
            image_path TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS media_credits (
            media_type TEXT NOT NULL,       -- 'movie', 'show', 'anime'
            item_id TEXT NOT NULL,          -- imdb_id
            person_id INTEGER NOT NULL,     -- references people(id)
            role TEXT NOT NULL,             -- 'cast', 'crew', 'creator', 'director'
            billing_order INTEGER,          -- 1, 2, 3...
            FOREIGN KEY (person_id) REFERENCES people(id)
        );
    """)

    # Cache dictionaries to avoid re-inserting duplicates
    # tmdb_id -> person_id
    tmdb_to_pid = {}
    # name_lower -> person_id (for people without tmdb_id, like anime directors)
    name_to_pid = {}

    people_batch = []
    credits_batch = []
    pid_counter = 1

    def get_or_create_person(tmdb_id, name, image_path):
        nonlocal pid_counter
        clean_name = (name or "").strip()
        if not clean_name:
            return None

        clean_tmdb = (
            int(tmdb_id) if tmdb_id is not None and str(tmdb_id).isdigit() else None
        )
        clean_img = (image_path or "").strip() or None

        # 1. Match by TMDb ID
        if clean_tmdb and clean_tmdb in tmdb_to_pid:
            return tmdb_to_pid[clean_tmdb]

        # 2. Match by normalized name
        name_key = clean_name.lower()
        if name_key in name_to_pid:
            existing_pid = name_to_pid[name_key]
            if clean_tmdb and clean_tmdb not in tmdb_to_pid:
                tmdb_to_pid[clean_tmdb] = existing_pid
            return existing_pid

        # 3. Create new person
        assigned_id = pid_counter
        pid_counter += 1

        if clean_tmdb:
            tmdb_to_pid[clean_tmdb] = assigned_id
        name_to_pid[name_key] = assigned_id

        people_batch.append((assigned_id, clean_tmdb, clean_name, clean_img))
        return assigned_id

    def safe_json_loads(val):
        if not val or val == "nan":
            return []
        try:
            return orjson.loads(val)
        except Exception:
            return []

    # ==========================================
    # 2. PROCESS MOVIES (Cast & Crew)
    # ==========================================
    print("  [*] Processing movies...", flush=True)
    cur.execute("""
        SELECT imdb_id, casts, casts_id, casts_image_path, crews, crews_id, crews_image_path
        FROM movies
        WHERE imdb_id IS NOT NULL;
    """)

    for row in cur.fetchall():
        imdb_id, c_names, c_ids, c_imgs, cr_names, cr_ids, cr_imgs = row

        # Cast
        names = safe_json_loads(c_names)
        ids = safe_json_loads(c_ids)
        imgs = safe_json_loads(c_imgs)
        for order, (n, i, img) in enumerate(zip(names, ids, imgs), start=1):
            pid = get_or_create_person(i, n, img)
            if pid:
                credits_batch.append(("movie", imdb_id, pid, "cast", order))

        # Crew
        names = safe_json_loads(cr_names)
        ids = safe_json_loads(cr_ids)
        imgs = safe_json_loads(cr_imgs)
        for order, (n, i, img) in enumerate(zip(names, ids, imgs), start=1):
            pid = get_or_create_person(i, n, img)
            if pid:
                credits_batch.append(("movie", imdb_id, pid, "crew", order))

    # ==========================================
    # 3. PROCESS SHOWS (Cast, Crew, Creators)
    # ==========================================
    print("  [*] Processing TV shows...", flush=True)
    cur.execute("""
        SELECT imdb_id,
               showrunner_and_creator, showrunner_and_creator_id, showrunner_and_creator_image,
               casts, casts_id, casts_image,
               crews, crews_id, crews_image
        FROM shows
        WHERE imdb_id IS NOT NULL;
    """)

    for row in cur.fetchall():
        (
            imdb_id,
            cr_names,
            cr_ids,
            cr_imgs,
            c_names,
            c_ids,
            c_imgs,
            cw_names,
            cw_ids,
            cw_imgs,
        ) = row

        # Creators / Showrunners
        names = safe_json_loads(cr_names)
        ids = safe_json_loads(cr_ids)
        imgs = safe_json_loads(cr_imgs)
        for order, (n, i, img) in enumerate(zip(names, ids, imgs), start=1):
            pid = get_or_create_person(i, n, img)
            if pid:
                credits_batch.append(("show", imdb_id, pid, "creator", order))

        # Cast
        names = safe_json_loads(c_names)
        ids = safe_json_loads(c_ids)
        imgs = safe_json_loads(c_imgs)
        for order, (n, i, img) in enumerate(zip(names, ids, imgs), start=1):
            pid = get_or_create_person(i, n, img)
            if pid:
                credits_batch.append(("show", imdb_id, pid, "cast", order))

        # Crew
        names = safe_json_loads(cw_names)
        ids = safe_json_loads(cw_ids)
        imgs = safe_json_loads(cw_imgs)
        for order, (n, i, img) in enumerate(zip(names, ids, imgs), start=1):
            pid = get_or_create_person(i, n, img)
            if pid:
                credits_batch.append(("show", imdb_id, pid, "crew", order))

    # ==========================================
    # 4. PROCESS ANIME (Directors)
    # ==========================================
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='anime';")
    if cur.fetchone():
        print("  [*] Processing anime directors...", flush=True)
        cur.execute(
            "SELECT imdb_id, director, director_image FROM anime WHERE imdb_id IS NOT NULL AND director IS NOT NULL;"
        )
        for row in cur.fetchall():
            imdb_id, directors_str, dir_img = row
            if not directors_str:
                continue
            # Anime directors can be comma-separated: "Tetsurou Araki, Masashi Koizuka"
            for order, d_name in enumerate(directors_str.split(", "), start=1):
                clean_d = d_name.strip()
                if clean_d:
                    pid = get_or_create_person(None, clean_d, dir_img)
                    if pid:
                        credits_batch.append(("anime", imdb_id, pid, "director", order))

    # ==========================================
    # 5. BULK INSERT & INDEX
    # ==========================================
    print(
        f"[*] Inserting {len(people_batch):,} unique people and {len(credits_batch):,} credits...",
        flush=True,
    )

    cur.executemany("INSERT INTO people VALUES (?, ?, ?, ?);", people_batch)
    cur.executemany("INSERT INTO media_credits VALUES (?, ?, ?, ?, ?);", credits_batch)

    print("[*] Creating B-Tree indexes on people and credits...", flush=True)
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_people_id ON people(id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_people_tmdb ON people(tmdb_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_people_name ON people(name);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_credits_item ON media_credits(item_id, media_type);"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_credits_person ON media_credits(person_id);"
    )

    conn.commit()
    conn.close()

    elapsed = round(time.time() - start_time, 2)
    print(f"[+] People normalization completed successfully in {elapsed}s!", flush=True)


if __name__ == "__main__":
    normalize_people()
