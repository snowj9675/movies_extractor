#!/usr/bin/env python3
"""
Upload movies_all.csv + downloads_all.csv → NeonDB
Uses PostgreSQL COPY (fastest possible ingestion).
Pipe-separated arrays are split and stored as JSONB.
"""

import csv
import io
import json
import os
import psycopg2
from psycopg2.extras import execute_values
import time

DATABASE_URL   = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_2CXrFnmZcWA3@ep-still-tree-atibb61y.c-9.us-east-1.aws.neon.tech/neondb?sslmode=require"
)
CSV_DIR        = "csv_chunks"          # folder produced by scraper_v7.py
MOVIES_CSV     = f"{CSV_DIR}/movies_all.csv"
DOWNLOADS_CSV  = f"{CSV_DIR}/downloads_all.csv"
BATCH_SIZE     = 1000                  # rows per INSERT batch

DDL = """
CREATE TABLE IF NOT EXISTS movies (
    id              TEXT        PRIMARY KEY,
    href            TEXT,
    title           TEXT,
    cover_image     TEXT,
    date_posted     TEXT,
    page            INTEGER,
    content_type    TEXT,
    movie_name      TEXT,
    release_year    TEXT,
    format          TEXT,
    size            TEXT,
    original_lang   TEXT,
    quality         TEXT,
    genres          JSONB       DEFAULT '[]',
    cast_list       JSONB       DEFAULT '[]',
    imdb_rating     TEXT,
    season          TEXT,
    episodes        TEXT,
    synopsis        TEXT,
    categories      JSONB       DEFAULT '[]',
    screenshots     JSONB       DEFAULT '[]',
    watch_online    TEXT,
    scraped_at      TIMESTAMPTZ,
    detail_error    TEXT,
    inserted_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS movie_downloads (
    id              SERIAL      PRIMARY KEY,
    movie_id        TEXT        NOT NULL REFERENCES movies(id) ON DELETE CASCADE,
    quality_group   TEXT,
    resolution      TEXT,
    size            TEXT,
    label           TEXT,
    url             TEXT,
    UNIQUE (movie_id, url)
);

CREATE INDEX IF NOT EXISTS idx_movie_downloads_movie_id
    ON movie_downloads(movie_id);
CREATE INDEX IF NOT EXISTS idx_movies_release_year
    ON movies(release_year);
CREATE INDEX IF NOT EXISTS idx_movies_content_type
    ON movies(content_type);
"""


def pipe_to_jsonb(val: str) -> str:
    """Convert 'a|b|c' → JSON array string '["a","b","c"]'"""
    if not val or not val.strip():
        return "[]"
    parts = [p.strip() for p in val.split("|") if p.strip()]
    return json.dumps(parts, ensure_ascii=True)


def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def load_movies_csv(path) -> list[tuple]:
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                r["id"],
                r.get("href") or None,
                r.get("title") or None,
                r.get("cover_image") or None,
                r.get("date_posted") or None,
                safe_int(r.get("page")),
                r.get("content_type") or None,
                r.get("movie_name") or None,
                r.get("release_year") or None,
                r.get("format") or None,
                r.get("size") or None,
                r.get("original_lang") or None,
                r.get("quality") or None,
                pipe_to_jsonb(r.get("genres", "")),
                pipe_to_jsonb(r.get("cast", "")),
                r.get("imdb_rating") or None,
                r.get("season") or None,
                r.get("episodes") or None,
                r.get("synopsis") or None,
                pipe_to_jsonb(r.get("categories", "")),
                pipe_to_jsonb(r.get("screenshots", "")),
                r.get("watch_online") or None,
                r.get("scraped_at") or None,
                r.get("detail_error") or None,
            ))
    return rows


def load_downloads_csv(path) -> list[tuple]:
    rows = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            url = r.get("url", "").strip()
            if not url:
                continue
            rows.append((
                r["movie_id"],
                r.get("quality_group") or None,
                r.get("resolution") or None,
                r.get("size") or None,
                r.get("label") or None,
                url,
            ))
    return rows


def upsert_movies(conn, rows: list[tuple]):
    sql = """
    INSERT INTO movies (
        id, href, title, cover_image, date_posted, page, content_type,
        movie_name, release_year, format, size, original_lang, quality,
        genres, cast_list, imdb_rating, season, episodes, synopsis,
        categories, screenshots, watch_online, scraped_at, detail_error
    ) VALUES %s
    ON CONFLICT (id) DO UPDATE SET
        href=EXCLUDED.href, title=EXCLUDED.title,
        cover_image=EXCLUDED.cover_image, date_posted=EXCLUDED.date_posted,
        page=EXCLUDED.page, content_type=EXCLUDED.content_type,
        movie_name=EXCLUDED.movie_name, release_year=EXCLUDED.release_year,
        format=EXCLUDED.format, size=EXCLUDED.size,
        original_lang=EXCLUDED.original_lang, quality=EXCLUDED.quality,
        genres=EXCLUDED.genres::jsonb, cast_list=EXCLUDED.cast_list::jsonb,
        imdb_rating=EXCLUDED.imdb_rating, season=EXCLUDED.season,
        episodes=EXCLUDED.episodes, synopsis=EXCLUDED.synopsis,
        categories=EXCLUDED.categories::jsonb,
        screenshots=EXCLUDED.screenshots::jsonb,
        watch_online=EXCLUDED.watch_online, scraped_at=EXCLUDED.scraped_at,
        detail_error=EXCLUDED.detail_error
    """
    t0 = time.time()
    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        conn.commit()
        done = min(start + BATCH_SIZE, total)
        elapsed = time.time() - t0
        rps = done / elapsed if elapsed > 0 else 0
        print(f"  movies [{done:>7}/{total}]  {rps:>6.0f} rows/s", end="\r")
    print(f"\n  ✓ {total:,} movies upserted in {time.time()-t0:.1f}s")


def upsert_downloads(conn, rows: list[tuple]):
    sql = """
    INSERT INTO movie_downloads (movie_id, quality_group, resolution, size, label, url)
    VALUES %s
    ON CONFLICT (movie_id, url) DO UPDATE SET
        quality_group=EXCLUDED.quality_group,
        resolution=EXCLUDED.resolution,
        size=EXCLUDED.size,
        label=EXCLUDED.label
    """
    t0 = time.time()
    total = len(rows)
    for start in range(0, total, BATCH_SIZE):
        batch = rows[start:start + BATCH_SIZE]
        with conn.cursor() as cur:
            execute_values(cur, sql, batch, page_size=BATCH_SIZE)
        conn.commit()
        done = min(start + BATCH_SIZE, total)
        elapsed = time.time() - t0
        rps = done / elapsed if elapsed > 0 else 0
        print(f"  downloads [{done:>7}/{total}]  {rps:>6.0f} rows/s", end="\r")
    print(f"\n  ✓ {total:,} downloads upserted in {time.time()-t0:.1f}s")


def verify(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM movies;")
        mc = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM movie_downloads;")
        dc = cur.fetchone()[0]
        cur.execute("""
            SELECT content_type, COUNT(*) FROM movies
            GROUP BY content_type ORDER BY COUNT(*) DESC;
        """)
        breakdown = cur.fetchall()
    print(f"\n── DB Verification ──")
    print(f"  movies total:    {mc:,}")
    print(f"  downloads total: {dc:,}")
    for ctype, cnt in breakdown:
        print(f"  {ctype or 'unknown':<12} {cnt:,}")


def main():
    print(f"Loading {MOVIES_CSV} …")
    movie_rows = load_movies_csv(MOVIES_CSV)
    print(f"  {len(movie_rows):,} movie rows loaded")

    print(f"Loading {DOWNLOADS_CSV} …")
    dl_rows = load_downloads_csv(DOWNLOADS_CSV)
    print(f"  {len(dl_rows):,} download rows loaded")

    conn = psycopg2.connect(DATABASE_URL)
    try:
        print("\nCreating tables …")
        with conn.cursor() as cur:
            cur.execute(DDL)
        conn.commit()

        print("\nUpserting movies …")
        upsert_movies(conn, movie_rows)

        print("\nUpserting downloads …")
        upsert_downloads(conn, dl_rows)

        verify(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
