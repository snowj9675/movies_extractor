"""
moviehdtv.com — detail scraper  (v3, GitHub Actions edition)
-------------------------------------------------------------
Key differences from Colab v2:
  - movies.json fetched from GitHub raw URL (not local file)
  - TIME_LIMIT: stops scraping with ~60s to spare before GH Actions kills the job
  - On stop: commits are handled by the workflow's "Commit progress" step
  - No tqdm (not available in GH Actions by default)
  - SAVE_EVERY pushed lower (25) since GH runner disk I/O is fast
  - Chunk awareness: prints clear progress so GH Actions logs are readable

Resume logic:
  - checkpoint_details.json  → list of completed IDs (committed to repo)
  - movies_detailed.json     → output (committed to repo)
  Both are pulled fresh at job start via `actions/checkout`, so every run
  continues exactly where the previous run left off.

NeonDB schema (per movie):
  id TEXT PK, href TEXT, title TEXT, cover_image TEXT, date_posted TEXT,
  page INT, movie_name TEXT, release_year TEXT, format TEXT, size TEXT,
  original_lang TEXT, quality TEXT, genres TEXT[], synopsis TEXT,
  categories TEXT[], screenshots TEXT[], watch_online TEXT,
  downloads JSONB, scraped_at TIMESTAMPTZ, detail_error TEXT
"""

import json, os, re, random, signal, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
MOVIES_JSON_URL  = "https://github.com/snowj9675/movies_extractor/raw/refs/heads/main/movies.json"
OUTPUT_FILE      = "movies_detailed.json"
CHECKPOINT_FILE  = "checkpoint_details.json"

MAX_WORKERS      = 10          # GH Actions runners have 2 vCPUs; keep I/O-bound threads moderate
SAVE_EVERY       = 25          # commit buffer: save output every N completions
REQUEST_DELAY    = (0.5, 1.5)
MAX_RETRIES      = 3
TIMEOUT          = 20

# Stop scraping this many seconds before GH Actions hard-kills the job.
# Workflow timeout is 300 min → we stop at 295 min = 17700 seconds.
TIME_LIMIT_SECS  = 17700

INFO_KEYS = {
    "Movie Name", "Release Year", "Format",
    "Size", "Original language", "Quality", "Genres",
}
SKIP_CATEGORIES = {"HdMovieHub"}
SKIP_IMG_WORDS  = {"logo", "favicon", "emoji", "avatar", "banner", "templates"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Globals ───────────────────────────────────────────────────────────────────
_start_time   = time.monotonic()
_stop_flag    = threading.Event()   # set when time is up → workers drain gracefully

# ── Thread-local session ──────────────────────────────────────────────────────
_local = threading.local()

def get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


# ── I/O ───────────────────────────────────────────────────────────────────────

def load_json(path, default):
    p = Path(path)
    if p.exists() and p.stat().st_size > 2:
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except json.JSONDecodeError:
            print(f"[WARN] {path} corrupt — starting fresh", flush=True)
    return default


def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


# ── Helpers ───────────────────────────────────────────────────────────────────

def slug(url: str) -> str:
    return re.sub(r"\.html?$", "", url.rstrip("/").split("/")[-1])


def abs_img(src: str) -> str:
    if not src:
        return ""
    if src.startswith("http"):
        return src
    if src.startswith("//"):
        return "https:" + src
    return "https://moviehdtv.com" + src if src.startswith("/") else src


def elapsed() -> float:
    return time.monotonic() - _start_time


def fmt_time(secs: float) -> str:
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


# ── Fetch movies.json from GitHub ─────────────────────────────────────────────

def fetch_movies_list() -> list[dict]:
    """Download movies.json from the repo's raw URL."""
    print(f"[INFO] Fetching movies list from GitHub …", flush=True)
    pat = os.environ.get("GH_PAT", "")
    headers = {**HEADERS}
    if pat:
        headers["Authorization"] = f"Bearer {pat}"
    for attempt in range(1, 4):
        try:
            r = requests.get(MOVIES_JSON_URL, headers=headers, timeout=60)
            r.raise_for_status()
            data = r.json()
            print(f"[INFO] Fetched {len(data)} movies from GitHub", flush=True)
            return data
        except Exception as e:
            print(f"[WARN] Attempt {attempt}/3 failed: {e}", flush=True)
            time.sleep(5)
    print("[ERROR] Could not fetch movies.json — falling back to local file", flush=True)
    return load_json("movies.json", [])


# ── HTTP fetch for movie pages ─────────────────────────────────────────────────

def fetch(url: str) -> str | None:
    if _stop_flag.is_set():
        return None
    session = get_session()
    for attempt in range(1, MAX_RETRIES + 1):
        if _stop_flag.is_set():
            return None
        try:
            time.sleep(random.uniform(*REQUEST_DELAY))
            r = session.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r.text
        except requests.RequestException as e:
            if attempt == MAX_RETRIES:
                return None
            time.sleep(2 ** attempt)


# ── Parse ─────────────────────────────────────────────────────────────────────

def parse(html: str, base: dict) -> dict:
    soup = BeautifulSoup(html, "html.parser")

    # info block  (<strong>Key:</strong> value)
    info: dict[str, str] = {}
    for strong in soup.find_all("strong"):
        key = strong.get_text(strip=True).rstrip(":")
        if key in INFO_KEYS:
            sib = strong.next_sibling
            if sib:
                info[key] = (sib if isinstance(sib, str) else sib.get_text()).strip()

    # synopsis
    synopsis = ""
    h3 = soup.find("h3", string=lambda s: s and "SYNOPSIS" in s.upper())
    if h3:
        p = h3.find_next("p")
        if p:
            synopsis = p.get_text(strip=True)

    # screenshots
    cover = base.get("image", "")
    screenshots = []
    for img in soup.select(".container img"):
        src = abs_img(img.get("src", ""))
        if not src or src == cover:
            continue
        if any(w in src.lower() for w in SKIP_IMG_WORDS):
            continue
        if src not in screenshots:
            screenshots.append(src)

    # download links
    downloads = []
    seen_urls: set[str] = set()
    current_quality = None
    for tag in soup.select(".download-links-div h3, .download-links-div p"):
        if tag.name == "h3":
            span = tag.find("span")
            if span:
                current_quality = span.get_text(strip=True)
            else:
                a = tag.find("a")
                if a:
                    _append_dl(downloads, seen_urls, current_quality, a)
        else:
            for a in tag.find_all("a"):
                _append_dl(downloads, seen_urls, current_quality, a)

    # watch online
    watch_online = None
    iframe = soup.select_one("#IndStreamPlayer iframe")
    if iframe:
        watch_online = iframe.get("src") or iframe.get("data-src")

    # categories
    categories = []
    first_p = soup.select_one("div.full-text p, article p, .post-content p")
    if first_p:
        for a in first_p.find_all("a"):
            t = a.get_text(strip=True)
            if t and t not in SKIP_CATEGORIES and t not in categories:
                categories.append(t)

    # genres
    genres_raw = info.get("Genres", "")
    genres = [g.strip() for g in re.split(r"[,/|]", genres_raw) if g.strip()]

    # title: h1 > og:title > stub
    real_title = base.get("title", "")
    h1 = soup.select_one("h1.post-title, h1.entry-title, article h1, h1")
    if h1:
        real_title = h1.get_text(strip=True)
    if not real_title:
        og = soup.find("meta", property="og:title")
        if og:
            real_title = og.get("content", "").replace(" - HdMovieHub", "").strip()

    # cover: og:image > stub
    real_cover = base.get("image", "")
    if not real_cover:
        og_img = soup.find("meta", property="og:image")
        if og_img:
            real_cover = og_img.get("content", "").strip()

    # date: page tag > stub
    real_date = base.get("date", "")
    if not real_date:
        time_tag = soup.select_one("time, .date-time span, .post-date")
        if time_tag:
            real_date = time_tag.get("datetime") or time_tag.get_text(strip=True)

    return {
        "id":            slug(base["href"]),
        "href":          base["href"],
        "title":         real_title,
        "cover_image":   real_cover,
        "date_posted":   real_date,
        "page":          base.get("page"),
        "movie_name":    info.get("Movie Name", ""),
        "release_year":  info.get("Release Year", ""),
        "format":        info.get("Format", ""),
        "size":          info.get("Size", ""),
        "original_lang": info.get("Original language", ""),
        "quality":       info.get("Quality", ""),
        "genres":        genres,
        "synopsis":      synopsis,
        "categories":    categories,
        "screenshots":   screenshots,
        "watch_online":  watch_online,
        "downloads":     downloads,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "detail_error":  None,
    }


def _append_dl(downloads, seen_urls, quality_group, a_tag):
    url = a_tag.get("href", "").strip()
    if not url or url in seen_urls:
        return
    seen_urls.add(url)
    label = a_tag.get_text(" ", strip=True)
    res   = (re.search(r"\b(4[Kk]|2160p|1080p|720p|480p|360p|240p)\b", label) or [None, None])[1]
    sz    = (re.search(r"(\d+(?:\.\d+)?\s*(?:MB|GB|KB))", label, re.I) or [None, None])[1]
    downloads.append({
        "quality_group": quality_group,
        "resolution":    res,
        "size":          sz.replace(" ", "") if sz else None,
        "label":         label,
        "url":           url,
    })


# ── Worker ────────────────────────────────────────────────────────────────────

def process(base: dict) -> dict:
    movie_id = slug(base["href"])
    if _stop_flag.is_set():
        return {**base, "id": movie_id,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": "skipped_time_limit"}
    html = fetch(base["href"])
    if html is None:
        return {**base, "id": movie_id,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": "fetch_failed" if not _stop_flag.is_set() else "skipped_time_limit"}
    try:
        return parse(html, base)
    except Exception as e:
        return {**base, "id": movie_id,
                "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": f"parse_error: {e}"}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _start_time
    _start_time = time.monotonic()

    # Load inputs
    movies = fetch_movies_list()
    if not movies:
        print("[ERROR] No movies to process", flush=True)
        sys.exit(1)

    existing: list[dict]     = load_json(OUTPUT_FILE, [])
    result_map: dict[str, dict] = {r["id"]: r for r in existing if "id" in r}
    cp_ids: list             = load_json(CHECKPOINT_FILE, [])
    done_ids: set[str]       = set(cp_ids) | set(result_map.keys())

    print(f"[INFO] Total movies:    {len(movies)}", flush=True)
    print(f"[INFO] Already done:    {len(done_ids)}", flush=True)
    print(f"[INFO] In output file:  {len(result_map)}", flush=True)

    todo = [m for m in movies if slug(m["href"]) not in done_ids]
    print(f"[INFO] Remaining:       {len(todo)}", flush=True)
    print(f"[INFO] Time limit:      {fmt_time(TIME_LIMIT_SECS)}", flush=True)
    print(f"[INFO] Workers:         {MAX_WORKERS}", flush=True)

    if not todo:
        print("[INFO] Nothing to do — all movies already scraped.", flush=True)
        return

    total     = len(todo)
    completed = 0
    errors    = 0
    skipped   = 0
    save_lock = threading.Lock()
    pending   = [0]

    def do_save():
        save_json(OUTPUT_FILE,     list(result_map.values()))
        save_json(CHECKPOINT_FILE, list(done_ids))

    # Time-limit watcher thread — sets stop flag 60s before deadline
    def watcher():
        deadline = TIME_LIMIT_SECS - 60
        while not _stop_flag.is_set():
            if elapsed() >= deadline:
                print(f"\n[WARN] Time limit reached at {fmt_time(elapsed())} — draining workers …", flush=True)
                _stop_flag.set()
                return
            time.sleep(5)

    watcher_thread = threading.Thread(target=watcher, daemon=True)
    watcher_thread.start()

    LOG_EVERY = 50   # print a summary line every N completions

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process, m): m for m in todo}

        for future in as_completed(futures):
            try:
                detail = future.result()
            except Exception as exc:
                base = futures[future]
                detail = {
                    **base,
                    "id":           slug(base["href"]),
                    "scraped_at":   datetime.now(timezone.utc).isoformat(),
                    "detail_error": f"exception: {exc}",
                }

            movie_id = detail.get("id", "")
            err      = detail.get("detail_error")

            with save_lock:
                result_map[movie_id] = detail
                done_ids.add(movie_id)

                if not err:
                    completed += 1
                elif "skipped" in (err or ""):
                    skipped += 1
                else:
                    errors += 1

                pending[0] += 1
                if pending[0] >= SAVE_EVERY:
                    do_save()
                    pending[0] = 0

                total_done = completed + errors + skipped

                if total_done % LOG_EVERY == 0 or total_done == total:
                    pct = total_done / total * 100
                    print(
                        f"[{fmt_time(elapsed())}] {total_done}/{total} ({pct:.1f}%)  "
                        f"ok={completed}  err={errors}  skip={skipped}  "
                        f"out={len(result_map)}",
                        flush=True
                    )

            # Stop dispatching new work once time limit hit
            if _stop_flag.is_set() and skipped > 0:
                break

    # Final flush
    with save_lock:
        do_save()

    _stop_flag.set()   # stop watcher

    print(f"\n[DONE] elapsed={fmt_time(elapsed())}", flush=True)
    print(f"       completed={completed}  errors={errors}  skipped={skipped}", flush=True)
    print(f"       movies_detailed.json total: {len(result_map)}", flush=True)

    if skipped > 0:
        print(f"[INFO] {skipped} movies skipped due to time limit — will resume next run", flush=True)
        # Exit 0 so the workflow's "Commit progress" step still runs
        sys.exit(0)


if __name__ == "__main__":
    main()
