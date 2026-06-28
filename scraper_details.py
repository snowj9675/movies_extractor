"""
moviehdtv.com / moviedbhub.com — detail scraper (v7)
------------------------------------------------------
Changes from v6:
  - Output is CSV instead of JSON — eliminates all JSON corruption issues
  - Two CSV files per chunk: movies_NNN.csv + downloads_NNN.csv
  - CHUNK_SIZE = 2000 hrefs per chunk pair
  - Checkpoint is a plain text file (one scraped ID per line)
  - Merge step at end produces movies_all.csv + downloads_all.csv
  - Arrays (genres, cast, screenshots, categories) stored pipe-separated (|)
  - All text fields go through sanitize_csv() — strips newlines, tabs, quotes
  - No JSON encoding/decoding anywhere in the hot path
"""

import csv
import os
import re
import random
import sys
import time
import threading
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
MOVIES_JSON_URL = "https://github.com/snowj9675/movies_extractor/raw/refs/heads/main/movies.json"
OUTPUT_DIR      = "csv_chunks"          # folder for chunk files
CHECKPOINT_FILE = "checkpoint_details.txt"   # one scraped ID per line
RESCRAPE_FILE   = "needs_rescrape.json"      # optional targeted re-scrape list

CHUNK_SIZE      = 2000    # hrefs per chunk pair (movies + downloads CSV)
MAX_WORKERS     = 10
SAVE_EVERY      = 50      # flush to CSV every N records
REQUEST_DELAY   = (0.5, 1.5)
MAX_RETRIES     = 3
TIMEOUT         = 20
TIME_LIMIT_SECS = 17700   # 295 min for GitHub Actions

# ── CSV column definitions ────────────────────────────────────────────────────
MOVIE_COLS = [
    "id", "href", "title", "cover_image", "date_posted", "page",
    "content_type", "movie_name", "release_year", "format", "size",
    "original_lang", "quality", "genres", "cast", "imdb_rating",
    "season", "episodes", "synopsis", "categories", "screenshots",
    "watch_online", "scraped_at", "detail_error",
]

DOWNLOAD_COLS = [
    "movie_id", "quality_group", "resolution", "size", "label", "url",
]

# ── Keyword / regex constants (unchanged from v6) ─────────────────────────────
INFO_KEYWORD_MAP = {
    "movie name":        "movie_name",
    "web-series name":   "movie_name",
    "series name":       "movie_name",
    "show name":         "movie_name",
    "release year":      "release_year",
    "format":            "format",
    "size":              "size",
    "original language": "original_lang",
    "quality":           "quality",
    "genres":            "genres",
    "genre":             "genres",
    "cast":              "cast",
    "season":            "season",
    "episodes":          "episodes",
    "imdb rating":       "imdb_rating",
}

QUALITY_KEYWORDS = re.compile(
    r"\b(4k|2160p|1080p|720p|480p|360p|240p|hdtc|hdrip|webrip|web-dl|bdrip|dvdrip|hevc|x264|x265)\b",
    re.IGNORECASE,
)

SKIP_IMG_WORDS = {"logo", "favicon", "emoji", "avatar", "banner", "templates"}
KNOWN_CDN_HOSTS = {
    "nexdrive", "gdrive", "drive.google", "mega.nz", "mediafire",
    "pixeldrain", "1fichier", "gofile", "buzzheavier", "hubcloud",
    "driveseed", "filepress", "send.cm", "uploadhaven",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

_start_time = time.monotonic()
_stop_flag  = threading.Event()
_local      = threading.local()


# ── Text sanitizers ───────────────────────────────────────────────────────────

def sanitize_csv(value) -> str:
    """
    Makes any value safe for CSV storage:
      - Coerces to str
      - NFC unicode normalization
      - Strips null bytes and control chars (keeps space)
      - Collapses all whitespace (newlines, tabs) to a single space
      - Strips leading/trailing whitespace
    CSV quoting handles commas/quotes — we just kill newlines/tabs
    that would break row boundaries.
    """
    if value is None:
        return ""
    if not isinstance(value, str):
        value = str(value)
    value = unicodedata.normalize("NFC", value)
    # Remove ALL control chars including \n \r \t — CSV rows must be single-line
    value = re.sub(r"[\x00-\x1f\x7f\x80-\x9f]", " ", value)
    value = re.sub(r" +", " ", value).strip()
    return value


def sanitize_url(value) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[\x00-\x1f\x7f]", "", value).strip()


def pipe_join(lst) -> str:
    """Join a list into a pipe-separated string for CSV storage."""
    if not lst:
        return ""
    return "|".join(sanitize_csv(str(x)) for x in lst if x)


# ── Session / IO helpers ──────────────────────────────────────────────────────

def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


def load_checkpoint() -> set:
    p = Path(CHECKPOINT_FILE)
    if p.exists():
        return set(p.read_text(encoding="utf-8").splitlines())
    return set()


def append_checkpoint(movie_id: str):
    with open(CHECKPOINT_FILE, "a", encoding="utf-8") as f:
        f.write(movie_id + "\n")


def chunk_path(chunk_num: int, kind: str) -> Path:
    """Returns e.g. csv_chunks/movies_001.csv"""
    Path(OUTPUT_DIR).mkdir(exist_ok=True)
    return Path(OUTPUT_DIR) / f"{kind}_{chunk_num:03d}.csv"


def open_csv_writers(chunk_num: int):
    """Open (append) CSV writers for movies + downloads chunk."""
    mp = chunk_path(chunk_num, "movies")
    dp = chunk_path(chunk_num, "downloads")

    write_movie_header    = not mp.exists()
    write_download_header = not dp.exists()

    mf = open(mp, "a", encoding="utf-8", newline="")
    df = open(dp, "a", encoding="utf-8", newline="")

    mw = csv.DictWriter(mf, fieldnames=MOVIE_COLS, extrasaction="ignore")
    dw = csv.DictWriter(df, fieldnames=DOWNLOAD_COLS, extrasaction="ignore")

    if write_movie_header:
        mw.writeheader()
    if write_download_header:
        dw.writeheader()

    return mf, df, mw, dw


def load_json_file(path, default):
    import json
    p = Path(path)
    if p.exists() and p.stat().st_size > 2:
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            print(f"[WARN] {path} unreadable", flush=True)
    return default


def slug(url):
    return re.sub(r"\.html?$", "", url.rstrip("/").split("/")[-1])


def abs_img(src):
    if not src: return ""
    if src.startswith("http"): return src
    if src.startswith("//"): return "https:" + src
    if src.startswith("/"): return "https://moviehdtv.com" + src
    return src


def elapsed():
    return time.monotonic() - _start_time


def fmt_time(secs):
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def fetch_movies_list():
    import json
    print("[INFO] Fetching movies list …", flush=True)
    pat  = os.environ.get("GH_PAT", "")
    hdrs = {**HEADERS, **({"Authorization": f"Bearer {pat}"} if pat else {})}
    for attempt in range(1, 4):
        try:
            r = requests.get(MOVIES_JSON_URL, headers=hdrs, timeout=60)
            r.raise_for_status()
            data = r.json()
            print(f"[INFO] {len(data)} movies fetched", flush=True)
            return data
        except Exception as e:
            print(f"[WARN] Attempt {attempt}/3: {e}", flush=True)
            time.sleep(5)
    return load_json_file("movies.json", [])


def fetch(url):
    if _stop_flag.is_set(): return None
    session = get_session()
    for attempt in range(1, MAX_RETRIES + 1):
        if _stop_flag.is_set(): return None
        try:
            time.sleep(random.uniform(*REQUEST_DELAY))
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException:
            if attempt == MAX_RETRIES: return None
            time.sleep(2 ** attempt)


# ── Info parser (unchanged logic, sanitize_csv instead of sanitize_text) ──────

def extract_info(soup) -> dict:
    info = {}

    def store(raw_key, raw_value):
        key   = sanitize_csv(raw_key).lower().rstrip(":")
        field = INFO_KEYWORD_MAP.get(key)
        if field and raw_value and field not in info:
            info[field] = sanitize_csv(raw_value)

    for tag in soup.find_all("strong"):
        raw_key = tag.get_text(strip=True).rstrip(":")
        sib = tag.next_sibling
        if sib:
            val = (sib if isinstance(sib, str) else sib.get_text()).strip()
            store(raw_key, val)

    for tag in soup.find_all("b"):
        raw_key = tag.get_text(strip=True).rstrip(":")
        sib = tag.next_sibling
        if sib:
            val = (sib if isinstance(sib, str) else sib.get_text()).strip()
            val = val.lstrip(":").strip()
            store(raw_key, val)

    if "movie_name" not in info:
        for p in soup.find_all(["p", "li"]):
            text = p.get_text(separator="\n")
            for line in text.splitlines():
                line = line.strip().lstrip("*👉").strip()
                if ":" in line:
                    parts = line.split(":", 1)
                    store(parts[0], parts[1])

    return info


# ── Download parser ───────────────────────────────────────────────────────────

def extract_downloads(soup) -> list:
    downloads = []
    seen_urls: set = set()

    def append(quality_group, a_tag):
        url = sanitize_url(a_tag.get("href", ""))
        if not url or url in seen_urls or url.startswith("#"):
            return
        if not any(cdn in url for cdn in KNOWN_CDN_HOSTS):
            if url.startswith("http") and \
               "moviehdtv.com" not in url and "moviedbhub.com" not in url:
                pass
            else:
                return
        seen_urls.add(url)

        label   = sanitize_csv(a_tag.get_text(" ", strip=True))
        q_group = sanitize_csv(quality_group) if quality_group else ""

        res = (re.search(r"\b(4[Kk]|2160p|1080p|720p|480p|360p|240p)\b", label)
               or [None, None])[1]
        sz  = (re.search(r"(\d+(?:\.\d+)?\s*(?:MB|GB|KB))", label, re.I)
               or [None, None])[1]

        downloads.append({
            "quality_group": q_group,
            "resolution":    res or "",
            "size":          sz.replace(" ", "") if sz else "",
            "label":         label,
            "url":           url,
        })

    dl_div = soup.select_one(".download-links-div")
    if dl_div:
        current_q = None
        for tag in dl_div.find_all(["h3", "h4", "p", "li"]):
            if tag.name in ("h3", "h4"):
                span = tag.find("span")
                a    = tag.find("a")
                if span and not a:
                    current_q = sanitize_csv(span.get_text(strip=True))
                elif a:
                    append(current_q, a)
                elif QUALITY_KEYWORDS.search(tag.get_text()):
                    current_q = sanitize_csv(tag.get_text(strip=True))
            else:
                for a in tag.find_all("a", href=True):
                    append(current_q, a)

    if not downloads:
        current_q = None
        for tag in soup.find_all("h3"):
            a    = tag.find("a", href=True)
            text = tag.get_text(strip=True)
            if a:
                append(current_q, a)
            elif QUALITY_KEYWORDS.search(text):
                current_q = sanitize_csv(text)

    if not downloads:
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if any(cdn in href for cdn in KNOWN_CDN_HOSTS):
                append(None, a)

    return downloads


# ── Full page parser ──────────────────────────────────────────────────────────

def parse(html, base) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    info = extract_info(soup)

    content_type = "series" if any(
        k in info for k in ["season", "episodes"]
    ) or "season" in base.get("href", "").lower() else "movie"

    synopsis = ""
    h3 = soup.find(
        lambda t: t.name == "h3" and t.get_text() and
        "SYNOPSIS" in t.get_text().upper()
    )
    if h3:
        p = h3.find_next("p")
        if p:
            synopsis = sanitize_csv(p.get_text(strip=True))

    cover       = base.get("image", "")
    screenshots = []
    for img in soup.find_all("img"):
        src = abs_img(img.get("src", ""))
        if not src or src == cover: continue
        if any(w in src.lower() for w in SKIP_IMG_WORDS): continue
        if "/uploads/" in src or "/posts/" in src:
            if src not in screenshots:
                screenshots.append(sanitize_url(src))

    downloads = extract_downloads(soup)

    watch_online = ""
    for sel in [
        "#IndStreamPlayer iframe", ".stream-player iframe",
        ".online-player iframe", "iframe[src*='player']",
        "iframe[src*='embed']", "iframe[src*='stream']",
    ]:
        iframe = soup.select_one(sel)
        if iframe:
            raw_src = iframe.get("src") or iframe.get("data-src")
            if raw_src:
                watch_online = sanitize_url(raw_src)
                break

    categories = []
    for p in soup.select("article p, .post-content p, div.full-text p"):
        for a in p.find_all("a", href=True):
            href_a = a["href"]
            t      = sanitize_csv(a.get_text(strip=True))
            if not t or t in {"HdMovieHub", "HdMovieHub.You"}: continue
            if "moviehdtv.com" in href_a or "moviedbhub.com" in href_a:
                if t not in categories:
                    categories.append(t)
        if categories: break

    genres_raw = info.get("genres", "")
    genres = [
        sanitize_csv(g)
        for g in re.split(r"[,/|]", genres_raw)
        if g.strip()
    ]

    quality = info.get("quality", "")
    if not quality and downloads:
        qs = [d["resolution"] for d in downloads if d.get("resolution")]
        if qs:
            quality = " / ".join(
                sorted(set(qs),
                       key=lambda x: int(re.sub(r"\D", "", x) or 0),
                       reverse=True)
            )

    real_title = sanitize_csv(base.get("title", ""))
    h1 = soup.select_one("h1.post-title, h1.entry-title, article h1, h1")
    if h1:
        real_title = sanitize_csv(h1.get_text(strip=True))
    if not real_title:
        og = soup.find("meta", property="og:title")
        if og:
            real_title = sanitize_csv(
                og.get("content", "").replace(" - HdMovieHub", "").strip()
            )

    real_cover = sanitize_url(base.get("image", ""))
    if not real_cover:
        og_img = soup.find("meta", property="og:image")
        if og_img:
            real_cover = sanitize_url(og_img.get("content", "").strip())

    real_date = sanitize_csv(base.get("date", ""))
    if not real_date:
        t = soup.select_one("time, .date-time span, .post-date")
        if t:
            real_date = sanitize_csv(
                t.get("datetime") or t.get_text(strip=True)
            )

    movie_id = slug(base["href"])

    # Flat movie row — arrays stored as pipe-separated strings
    movie_row = {
        "id":            movie_id,
        "href":          sanitize_url(base["href"]),
        "title":         real_title,
        "cover_image":   real_cover,
        "date_posted":   real_date,
        "page":          str(base.get("page", "")),
        "content_type":  content_type,
        "movie_name":    info.get("movie_name", ""),
        "release_year":  info.get("release_year", ""),
        "format":        info.get("format", ""),
        "size":          info.get("size", ""),
        "original_lang": info.get("original_lang", ""),
        "quality":       sanitize_csv(quality),
        "genres":        pipe_join(genres),
        "cast":          sanitize_csv(info.get("cast", "")),
        "imdb_rating":   sanitize_csv(info.get("imdb_rating", "")),
        "season":        sanitize_csv(info.get("season", "")),
        "episodes":      sanitize_csv(info.get("episodes", "")),
        "synopsis":      synopsis,
        "categories":    pipe_join(categories),
        "screenshots":   pipe_join(screenshots),
        "watch_online":  watch_online,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "detail_error":  "",
    }

    # Download rows — one per link
    download_rows = [
        {
            "movie_id":      movie_id,
            "quality_group": d["quality_group"],
            "resolution":    d["resolution"],
            "size":          d["size"],
            "label":         d["label"],
            "url":           d["url"],
        }
        for d in downloads
    ]

    return movie_row, download_rows


# ── Worker ────────────────────────────────────────────────────────────────────

def process(base):
    movie_id = slug(base["href"])

    if _stop_flag.is_set():
        return {
            "id": movie_id, "href": sanitize_url(base["href"]),
            "title": "", "cover_image": "", "date_posted": "",
            "page": str(base.get("page", "")), "content_type": "",
            "movie_name": "", "release_year": "", "format": "",
            "size": "", "original_lang": "", "quality": "",
            "genres": "", "cast": "", "imdb_rating": "",
            "season": "", "episodes": "", "synopsis": "",
            "categories": "", "screenshots": "", "watch_online": "",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "detail_error": "skipped_time_limit",
        }, []

    html = fetch(base["href"])
    if html is None:
        err = "fetch_failed" if not _stop_flag.is_set() else "skipped_time_limit"
        return {
            "id": movie_id, "href": sanitize_url(base["href"]),
            "title": "", "cover_image": "", "date_posted": "",
            "page": str(base.get("page", "")), "content_type": "",
            "movie_name": "", "release_year": "", "format": "",
            "size": "", "original_lang": "", "quality": "",
            "genres": "", "cast": "", "imdb_rating": "",
            "season": "", "episodes": "", "synopsis": "",
            "categories": "", "screenshots": "", "watch_online": "",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "detail_error": err,
        }, []

    try:
        movie_row, download_rows = parse(html, base)
        return movie_row, download_rows
    except Exception as e:
        return {
            "id": movie_id, "href": sanitize_url(base["href"]),
            "title": "", "cover_image": "", "date_posted": "",
            "page": str(base.get("page", "")), "content_type": "",
            "movie_name": "", "release_year": "", "format": "",
            "size": "", "original_lang": "", "quality": "",
            "genres": "", "cast": "", "imdb_rating": "",
            "season": "", "episodes": "", "synopsis": "",
            "categories": "", "screenshots": "", "watch_online": "",
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "detail_error": f"parse_error: {sanitize_csv(str(e))}",
        }, []


# ── Merge helper ──────────────────────────────────────────────────────────────

def merge_chunks():
    """Merge all chunk CSVs into movies_all.csv and downloads_all.csv."""
    out_dir = Path(OUTPUT_DIR)
    for kind, cols in [("movies", MOVIE_COLS), ("downloads", DOWNLOAD_COLS)]:
        out_path = out_dir / f"{kind}_all.csv"
        seen_ids: set = set()
        count = 0
        with open(out_path, "w", encoding="utf-8", newline="") as out_f:
            writer = csv.DictWriter(out_f, fieldnames=cols)
            writer.writeheader()
            for chunk_file in sorted(out_dir.glob(f"{kind}_[0-9]*.csv")):
                with open(chunk_file, encoding="utf-8", newline="") as in_f:
                    reader = csv.DictReader(in_f)
                    for row in reader:
                        dedup_key = row.get("id") if kind == "movies" else row.get("url")
                        if dedup_key and dedup_key in seen_ids:
                            continue
                        seen_ids.add(dedup_key)
                        writer.writerow(row)
                        count += 1
        print(f"[MERGE] {out_path}  →  {count:,} rows", flush=True)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _start_time
    _start_time = time.monotonic()

    Path(OUTPUT_DIR).mkdir(exist_ok=True)

    done_ids = load_checkpoint()

    rescrape_list = load_json_file(RESCRAPE_FILE, None)

    if rescrape_list is not None:
        print(f"[INFO] TARGETED MODE — {len(rescrape_list)} URLs", flush=True)
        todo     = [m for m in rescrape_list]
        done_ids = set()  # re-scrape all listed
    else:
        movies = fetch_movies_list()
        todo   = [m for m in movies if slug(m["href"]) not in done_ids]

    total = len(todo)
    print(f"[INFO] To scrape:    {total}",          flush=True)
    print(f"[INFO] Already done: {len(done_ids)}",  flush=True)
    print(f"[INFO] Chunk size:   {CHUNK_SIZE}",     flush=True)
    print(f"[INFO] Time limit:   {fmt_time(TIME_LIMIT_SECS)}", flush=True)

    if not todo:
        print("[INFO] Nothing to do — merging existing chunks.", flush=True)
        merge_chunks()
        return

    # Figure out which chunk number to start from
    existing_chunks = sorted(Path(OUTPUT_DIR).glob("movies_[0-9]*.csv"))
    if existing_chunks:
        last_num = int(existing_chunks[-1].stem.split("_")[-1])
        # Count rows in last chunk (minus header)
        with open(existing_chunks[-1], encoding="utf-8") as f:
            last_chunk_rows = sum(1 for _ in f) - 1
        if last_chunk_rows < CHUNK_SIZE:
            current_chunk = last_num
            chunk_count   = last_chunk_rows
        else:
            current_chunk = last_num + 1
            chunk_count   = 0
    else:
        current_chunk = 1
        chunk_count   = 0

    save_lock    = threading.Lock()
    pending_rows = []   # [(movie_row, [download_rows])]
    completed = errors = skipped = 0
    total_written = 0

    def flush_to_csv(rows):
        nonlocal current_chunk, chunk_count, total_written
        mf, df, mw, dw = open_csv_writers(current_chunk)
        try:
            for movie_row, download_rows in rows:
                mw.writerow(movie_row)
                chunk_count += 1
                total_written += 1
                for dr in download_rows:
                    dw.writerow(dr)
                if chunk_count >= CHUNK_SIZE:
                    mf.close(); df.close()
                    print(
                        f"\n[CHUNK] Closed chunk {current_chunk:03d} "
                        f"({CHUNK_SIZE} movies)", flush=True
                    )
                    current_chunk += 1
                    chunk_count    = 0
                    mf, df, mw, dw = open_csv_writers(current_chunk)
        finally:
            mf.close()
            df.close()

    def watcher():
        while not _stop_flag.is_set():
            if elapsed() >= TIME_LIMIT_SECS - 60:
                print(
                    f"\n[WARN] Time limit at {fmt_time(elapsed())} — draining …",
                    flush=True,
                )
                _stop_flag.set()
                return
            time.sleep(5)

    threading.Thread(target=watcher, daemon=True).start()

    LOG_EVERY = 50

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process, m): m for m in todo}
        for future in as_completed(futures):
            try:
                movie_row, download_rows = future.result()
            except Exception as exc:
                base     = futures[future]
                movie_id = slug(base["href"])
                movie_row = {
                    "id": movie_id, "href": sanitize_url(base["href"]),
                    "title": "", "cover_image": "", "date_posted": "",
                    "page": str(base.get("page", "")), "content_type": "",
                    "movie_name": "", "release_year": "", "format": "",
                    "size": "", "original_lang": "", "quality": "",
                    "genres": "", "cast": "", "imdb_rating": "",
                    "season": "", "episodes": "", "synopsis": "",
                    "categories": "", "screenshots": "", "watch_online": "",
                    "scraped_at": datetime.now(timezone.utc).isoformat(),
                    "detail_error": f"exception: {sanitize_csv(str(exc))}",
                }
                download_rows = []

            movie_id = movie_row["id"]
            err      = movie_row.get("detail_error", "")

            with save_lock:
                done_ids.add(movie_id)
                append_checkpoint(movie_id)

                if not err:
                    completed += 1
                elif "skipped" in err:
                    skipped   += 1
                else:
                    errors    += 1

                pending_rows.append((movie_row, download_rows))

                if len(pending_rows) >= SAVE_EVERY:
                    flush_to_csv(pending_rows)
                    pending_rows.clear()

                total_done = completed + errors + skipped
                if total_done % LOG_EVERY == 0 or total_done == total:
                    pct = total_done / total * 100
                    print(
                        f"[{fmt_time(elapsed())}] {total_done}/{total} ({pct:.1f}%)  "
                        f"ok={completed} err={errors} skip={skipped} "
                        f"written={total_written}",
                        flush=True,
                    )

            if _stop_flag.is_set() and skipped > 0:
                break

    # Flush any remaining
    with save_lock:
        if pending_rows:
            flush_to_csv(pending_rows)
            pending_rows.clear()

    _stop_flag.set()
    print(
        f"\n[DONE] {fmt_time(elapsed())}  "
        f"ok={completed} err={errors} skip={skipped}  "
        f"written={total_written}",
        flush=True,
    )

    print("\n[MERGE] Merging all chunks …", flush=True)
    merge_chunks()

    if rescrape_list is not None and skipped == 0:
        Path(RESCRAPE_FILE).unlink(missing_ok=True)
        print(f"[INFO] Removed {RESCRAPE_FILE}", flush=True)


if __name__ == "__main__":
    main()
