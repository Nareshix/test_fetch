import csv
import json
import os
import sqlite3
import sys
import time
import urllib.request
from collections import defaultdict, deque

DATA_DIR = "data"
os.makedirs(DATA_DIR, exist_ok=True)
FRIBB_JSON_PATH = os.path.join(DATA_DIR, "anime-list-mini.json")
CSV_PATH = os.path.join(DATA_DIR, "anilist_anime_data_complete.csv")
DB_PATH = "media.db"

csv.field_size_limit(2147483647)


def download_prerequisites():
    if not os.path.exists(FRIBB_JSON_PATH):
        print("[*] Downloading Fribb mapping file...", flush=True)
        urllib.request.urlretrieve(
            "https://raw.githubusercontent.com/Fribb/anime-lists/master/anime-list-mini.json",
            FRIBB_JSON_PATH,
        )


def clean_sub_title(full_title, root_title, format_str=""):
    if not root_title:
        return full_title
    lower_full = full_title.lower()
    lower_root = root_title.lower().strip()
    if lower_full.startswith(lower_root):
        rest = full_title[len(lower_root) :].lstrip(": -–— \t")
        if rest:
            if rest.isdigit():
                return f"Movie {rest}" if format_str == "MOVIE" else f"Part {rest}"
            return rest
    return full_title


def extract_part_label(title):
    lower = title.lower()
    if "final chapters" in lower or "kanketsu-hen" in lower:
        return "Final Chapters"
    if "part 2" in lower or "cour 2" in lower:
        return "Part 2"
    if "part 3" in lower or "cour 3" in lower:
        return "Part 3"
    if "part 4" in lower or "cour 4" in lower:
        return "Part 4"
    return None


def run_anime_pipeline():
    download_prerequisites()
    if not os.path.exists(CSV_PATH):
        print(
            f"[-] Missing {CSV_PATH}. Place complete AniList export in data/ folder.",
            flush=True,
        )
        return

    print("[*] Loading Fribb mapping...", flush=True)
    with open(FRIBB_JSON_PATH, "r", encoding="utf-8") as f:
        fribb_entries = json.load(f)

    fribb_map = {}
    for entry in fribb_entries:
        a_id = entry.get("anilist_id")
        imdb_list = entry.get("imdb_id") or []
        imdb_id = next((s for s in imdb_list if s.startswith("tt")), None)
        if a_id and imdb_id:
            tmdb = entry.get("themoviedb_id") or {}
            tv_id = tmdb.get("tv")
            movie_val = tmdb.get("movie")
            movie_id = (
                movie_val[0]
                if isinstance(movie_val, list) and movie_val
                else (movie_val if isinstance(movie_val, int) else None)
            )
            is_tv = tv_id is not None or entry.get("type") == "TV"
            fribb_map[a_id] = {
                "imdb_id": imdb_id,
                "tmdb_id": tv_id if is_tv else movie_id,
                "is_tv": is_tv,
            }

    print(f"[*] Processing AniList graph from {CSV_PATH}...", flush=True)
    media_store = {}
    raw_recs_map = {}

    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                a_id = int(row["id"])
            except Exception:
                continue

            fmt = row.get("format") or "TV"
            if fmt == "MUSIC":
                continue

            try:
                relations = json.loads(row.get("relations") or "[]")
            except Exception:
                relations = []

            try:
                recs_list = json.loads(row.get("recommendations") or "[]")
                parsed_recs = []
                for item in recs_list:
                    node = item.get("node") or {}
                    rating = node.get("rating") or 0
                    if rating > 0 and node.get("mediaRecommendation", {}).get("id"):
                        parsed_recs.append(
                            (rating, int(node["mediaRecommendation"]["id"]))
                        )
                parsed_recs.sort(key=lambda x: x[0], reverse=True)
                raw_recs_map[a_id] = [r_id for _, r_id in parsed_recs]
            except Exception:
                raw_recs_map[a_id] = []

            studio_name = None
            try:
                studios_list = json.loads(row.get("studios") or "[]")
                if studios_list:
                    studio_name = studios_list[0].get("name")
            except Exception:
                pass

            y = row.get("startDate_year")
            m = row.get("startDate_month")
            d = row.get("startDate_day")
            start_date = (
                f"{int(float(y)):04d}-{int(float(m)) if m and m != 'nan' else 1:02d}-{int(float(d)) if d and d != 'nan' else 1:02d}"
                if y and y != "nan"
                else None
            )

            score_val = row.get("averageScore")
            rating = (
                round(float(score_val) / 10.0, 1)
                if score_val and score_val != "nan"
                else None
            )

            ep_val = row.get("episodes")
            episodes = int(float(ep_val)) if ep_val and ep_val != "nan" else None

            media_store[a_id] = {
                "id": a_id,
                "title_english": row.get("title_english")
                if row.get("title_english") != "nan"
                else None,
                "title_romaji": row.get("title_romaji") or "Unknown",
                "format": fmt,
                "episodes": episodes,
                "status": row.get("status"),
                "rating": rating,
                "banner_url": row.get("bannerImage")
                if row.get("bannerImage") != "nan"
                else None,
                "cover_url": row.get("coverImage_extraLarge")
                or row.get("coverImage_large"),
                "studio": studio_name,
                "start_date": start_date,
                "relations": relations,
            }

    # Build Prequel / Sequel Spines
    spine_adj = defaultdict(set)
    for m_id, m in media_store.items():
        for rel in m["relations"]:
            if (rel.get("node") or {}).get("type") == "ANIME":
                rel_type = rel.get("relationType")
                target_id = (rel.get("node") or {}).get("id")
                if (
                    target_id
                    and target_id in media_store
                    and rel_type in ("PREQUEL", "SEQUEL")
                ):
                    spine_adj[m_id].add(target_id)
                    spine_adj[target_id].add(m_id)

    visited_spines = set()
    timeline_records = []

    for m_id in media_store:
        if m_id in visited_spines:
            continue
        spine = []
        q = deque([m_id])
        visited_spines.add(m_id)
        while q:
            curr = q.popleft()
            spine.append(curr)
            for neighbor in spine_adj[curr]:
                if neighbor not in visited_spines:
                    visited_spines.add(neighbor)
                    q.append(neighbor)

        spine_sorted = sorted(
            spine, key=lambda x: media_store[x].get("start_date") or "9999-99-99"
        )
        root_id = next(
            (
                cid
                for cid in spine_sorted
                if media_store[cid].get("format") in ("TV", "TV_SHORT", "ONA")
            ),
            spine_sorted[0],
        )
        root_media = media_store[root_id]
        root_title = (
            root_media.get("title_english") or root_media.get("title_romaji") or ""
        )

        branches = defaultdict(list)
        branches["main"] = spine_sorted
        seen_in_cluster = set(spine)

        for spine_member in spine:
            for rel in media_store[spine_member]["relations"]:
                if (rel.get("node") or {}).get("type") == "ANIME":
                    rel_type = rel.get("relationType")
                    target_id = (rel.get("node") or {}).get("id")
                    if (
                        target_id
                        and target_id in media_store
                        and target_id not in seen_in_cluster
                    ):
                        if rel_type == "SPIN_OFF":
                            branches["spinoff"].append(target_id)
                            seen_in_cluster.add(target_id)
                        elif rel_type == "SIDE_STORY":
                            branches["side"].append(target_id)
                            seen_in_cluster.add(target_id)
                        elif rel_type == "ALTERNATIVE":
                            branches["alt"].append(target_id)
                            seen_in_cluster.add(target_id)

        for b_type, member_ids in branches.items():
            member_ids.sort(
                key=lambda x: media_store[x].get("start_date") or "9999-99-99"
            )
            season_counter = 1
            for order, cid in enumerate(member_ids, start=1):
                m = media_store[cid]
                raw_title = m.get("title_english") or m.get("title_romaji") or "Unknown"
                fmt = m.get("format") or "TV"

                if b_type == "main" and fmt in ("TV", "TV_SHORT", "ONA"):
                    part = extract_part_label(raw_title)
                    display_title = (
                        f"Season {max(1, season_counter - 1)} ({part})"
                        if part
                        else f"Season {season_counter}"
                    )
                    if not part:
                        season_counter += 1
                else:
                    display_title = clean_sub_title(raw_title, root_title, fmt)

                timeline_records.append((root_id, cid, b_type, order, display_title))

    anime_records = []
    recommendation_records = []
    for a_id, m in media_store.items():
        fb = fribb_map.get(a_id, {})
        imdb_id = fb.get("imdb_id") or f"al:{a_id}"
        anime_records.append(
            (
                a_id,
                imdb_id,
                fb.get("tmdb_id"),
                1
                if fb.get("is_tv")
                else (1 if m.get("format") in ("TV", "TV_SHORT", "ONA") else 0),
                m["title_english"],
                m["title_romaji"],
                m["format"],
                m["episodes"],
                m["status"],
                m["rating"],
                m["banner_url"],
                m["cover_url"],
                m["studio"],
                m["start_date"],
            )
        )
        recs = raw_recs_map.get(a_id, [])
        for rank, rec_id in enumerate(recs[:12], start=1):
            if rec_id in media_store:
                recommendation_records.append((a_id, rec_id, rank))

    print(
        f"[*] Writing {len(anime_records):,} anime titles to SQLite at {DB_PATH}...",
        flush=True,
    )
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anime (
            anilist_id INTEGER PRIMARY KEY,
            imdb_id TEXT NOT NULL,
            tmdb_id INTEGER,
            is_tv BOOLEAN NOT NULL,
            title_english TEXT,
            title_romaji TEXT NOT NULL,
            format TEXT NOT NULL,
            episodes INTEGER,
            status TEXT,
            rating REAL,
            banner_url TEXT,
            cover_url TEXT,
            studio TEXT,
            start_date TEXT
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS franchise_timeline (
            root_id INTEGER NOT NULL,
            member_id INTEGER NOT NULL,
            branch_type TEXT NOT NULL,
            chronological_order INTEGER NOT NULL,
            display_title TEXT NOT NULL,
            PRIMARY KEY (root_id, member_id)
        );
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anime_recommendation (
            anime_id INTEGER NOT NULL,
            rec_id INTEGER NOT NULL,
            rank INTEGER NOT NULL,
            PRIMARY KEY (anime_id, rec_id)
        );
    """)
    cur.executemany(
        "INSERT OR REPLACE INTO anime VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
        anime_records,
    )
    cur.executemany(
        "INSERT OR REPLACE INTO franchise_timeline VALUES (?, ?, ?, ?, ?);",
        timeline_records,
    )
    cur.executemany(
        "INSERT OR REPLACE INTO anime_recommendation VALUES (?, ?, ?);",
        recommendation_records,
    )
    conn.commit()
    conn.close()
    print("[+] Anime tables built successfully!", flush=True)


if __name__ == "__main__":
    run_anime_pipeline()
