"""
moviehdtv.com / moviedbhub.com — detail scraper (v5)
------------------------------------------------------
Keyword-targeted parsing strategy:
  The site has TWO info block formats depending on page type:

  FORMAT A — Movies (uses <strong> tags):
    <strong>Movie Name:</strong> Pushpa 2
    <strong>Release Year:</strong> 2024

  FORMAT B — Series (uses <b> tags, plain text pattern):
    <b>Web-Series Name</b>: Vikram on Duty
    <b>Release Year:</b> 2026
    OR rendered as plain paragraph text:
    **Web-Series Name**: Vikram on Duty

  Download block also varies:
  STYLE A — .download-links-div wrapper:
    <div class="download-links-div">
      <h3><span>720p</span></h3>
      <h3><a href="...">Download</a></h3>

  STYLE B — bare h3 sequence (series pages):
    <h3>480p</h3>
    <h3><a href="...">⚡Click Here To Download⚡</a></h3>
    <h3>720p</h3>
    <h3><a href="...">⚡Click Here To Download⚡</a></h3>

  Both moviehdtv.com and moviedbhub.com use the same template.
"""

import json, os, re, random, sys, time, threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Config ────────────────────────────────────────────────────────────────────
MOVIES_JSON_URL = "https://github.com/snowj9675/movies_extractor/raw/refs/heads/main/movies.json"
OUTPUT_FILE     = "movies_detailed.json"
CHECKPOINT_FILE = "checkpoint_details.json"
RESCRAPE_FILE   = "needs_rescrape.json"

MAX_WORKERS     = 10
SAVE_EVERY      = 25
REQUEST_DELAY   = (0.5, 1.5)
MAX_RETRIES     = 3
TIMEOUT         = 20
TIME_LIMIT_SECS = 17700   # 295 min for GitHub Actions

# Keyword map: every possible label → unified field name
# Covers both <strong> and <b> variants, with/without colon
INFO_KEYWORD_MAP = {
    "movie name":       "movie_name",
    "web-series name":  "movie_name",
    "series name":      "movie_name",
    "show name":        "movie_name",
    "release year":     "release_year",
    "format":           "format",
    "size":             "size",
    "original language":"original_lang",
    "quality":          "quality",
    "genres":           "genres",
    "genre":            "genres",
    "cast":             "cast",
    "season":           "season",
    "episodes":         "episodes",
    "imdb rating":      "imdb_rating",
}

# Quality resolution keywords used to identify h3 headings in STYLE B
QUALITY_KEYWORDS = re.compile(
    r"\b(4k|2160p|1080p|720p|480p|360p|240p|hdtc|hdrip|webrip|web-dl|bdrip|dvdrip|hevc|x264|x265)\b",
    re.IGNORECASE
)

SKIP_IMG_WORDS  = {"logo", "favicon", "emoji", "avatar", "banner", "templates"}
KNOWN_CDN_HOSTS = {"nexdrive", "gdrive", "drive.google", "mega.nz", "mediafire",
                   "pixeldrain", "1fichier", "gofile", "buzzheavier", "hubcloud",
                   "driveseed", "filepress", "send.cm", "uploadhaven"}

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


def get_session():
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        _local.session = s
    return _local.session


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
    print("[INFO] Fetching movies list …", flush=True)
    pat = os.environ.get("GH_PAT", "")
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
    return load_json("movies.json", [])


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
        except requests.RequestException as e:
            if attempt == MAX_RETRIES: return None
            time.sleep(2 ** attempt)


# ── Core keyword-targeted info parser ────────────────────────────────────────

def extract_info(soup) -> dict:
    """
    Tries both FORMAT A (<strong>) and FORMAT B (<b> / plain text).
    Returns a flat dict keyed by our unified field names.
    """
    info = {}

    def store(raw_key, raw_value):
        key = raw_key.lower().strip().rstrip(":")
        field = INFO_KEYWORD_MAP.get(key)
        if field and raw_value and field not in info:
            info[field] = raw_value.strip()

    # FORMAT A — <strong>Key:</strong> value as next sibling
    for tag in soup.find_all("strong"):
        raw_key = tag.get_text(strip=True).rstrip(":")
        sib = tag.next_sibling
        if sib:
            val = (sib if isinstance(sib, str) else sib.get_text()).strip()
            store(raw_key, val)

    # FORMAT B — <b>Key</b>: value as next sibling
    for tag in soup.find_all("b"):
        raw_key = tag.get_text(strip=True).rstrip(":")
        sib = tag.next_sibling
        if sib:
            val = (sib if isinstance(sib, str) else sib.get_text()).strip()
            # strip leading colon/space from sibling text
            val = val.lstrip(":").strip()
            store(raw_key, val)

    # FORMAT B fallback — parse full paragraph text as "Key: Value" lines
    # Covers cases where bold is rendered as literal **text** or CSS bold
    if "movie_name" not in info:
        for p in soup.find_all(["p", "li"]):
            text = p.get_text(separator="\n")
            for line in text.splitlines():
                line = line.strip().lstrip("*👉").strip()
                if ":" in line:
                    parts = line.split(":", 1)
                    store(parts[0], parts[1])

    return info


# ── Download link parser ──────────────────────────────────────────────────────

def extract_downloads(soup) -> list:
    downloads = []
    seen_urls: set = set()

    def append(quality_group, a_tag):
        url = a_tag.get("href", "").strip()
        if not url or url in seen_urls or url.startswith("#"):
            return
        # Only keep actual download links, not internal nav links
        if not any(cdn in url for cdn in KNOWN_CDN_HOSTS) and \
           "moviehdtv.com" not in url and "moviedbhub.com" not in url:
            # Accept any external link that isn't the same domain
            if url.startswith("http") and \
               "moviehdtv.com" not in url and "moviedbhub.com" not in url:
                pass  # external = likely a download
            else:
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

    # STYLE A — .download-links-div
    dl_div = soup.select_one(".download-links-div")
    if dl_div:
        current_q = None
        for tag in dl_div.find_all(["h3", "h4", "p", "li"]):
            if tag.name in ("h3", "h4"):
                span = tag.find("span")
                a    = tag.find("a")
                if span and not a:
                    current_q = span.get_text(strip=True)
                elif a:
                    append(current_q, a)
                elif QUALITY_KEYWORDS.search(tag.get_text()):
                    current_q = tag.get_text(strip=True)
            else:
                for a in tag.find_all("a", href=True):
                    append(current_q, a)

    # STYLE B — bare <h3> sequence: quality heading then <h3><a> link
    # Only use if STYLE A found nothing
    if not downloads:
        current_q = None
        for tag in soup.find_all("h3"):
            a = tag.find("a", href=True)
            text = tag.get_text(strip=True)
            if a:
                # This h3 has a link — it's a download entry
                append(current_q, a)
            elif QUALITY_KEYWORDS.search(text):
                # This h3 is a quality heading
                current_q = text
            # else: some other h3, ignore

    # STYLE C — last resort: any external <a> pointing to CDN
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

    # content type
    content_type = "series" if any(
        k in info for k in ["season", "episodes"]
    ) or "season" in base.get("href", "").lower() else "movie"

    # synopsis
    synopsis = ""
    h3 = soup.find(lambda t: t.name == "h3" and t.get_text() and
                   "SYNOPSIS" in t.get_text().upper())
    if h3:
        p = h3.find_next("p")
        if p: synopsis = p.get_text(strip=True)

    # screenshots — images below the cover in article body
    cover = base.get("image", "")
    screenshots = []
    for img in soup.find_all("img"):
        src = abs_img(img.get("src", ""))
        if not src or src == cover: continue
        if any(w in src.lower() for w in SKIP_IMG_WORDS): continue
        # only posts/covers or uploads paths — skip template/UI images
        if "/uploads/" in src or "/posts/" in src:
            if src not in screenshots:
                screenshots.append(src)

    # downloads
    downloads = extract_downloads(soup)

    # watch online
    watch_online = None
    for sel in ["#IndStreamPlayer iframe", ".stream-player iframe",
                ".online-player iframe", "iframe[src*='player']",
                "iframe[src*='embed']", "iframe[src*='stream']"]:
        iframe = soup.select_one(sel)
        if iframe:
            watch_online = iframe.get("src") or iframe.get("data-src")
            if watch_online: break

    # categories — internal nav links in article paragraphs
    categories = []
    for p in soup.select("article p, .post-content p, div.full-text p"):
        for a in p.find_all("a", href=True):
            href_a = a["href"]
            t = a.get_text(strip=True)
            if not t or t in {"HdMovieHub", "HdMovieHub.You"}: continue
            if "moviehdtv.com" in href_a or "moviedbhub.com" in href_a:
                if t not in categories:
                    categories.append(t)
        if categories: break

    # genres — split from info
    genres_raw = info.get("genres", "")
    genres = [g.strip() for g in re.split(r"[,/|]", genres_raw) if g.strip()]

    # quality fallback from downloads
    quality = info.get("quality", "")
    if not quality and downloads:
        qs = [d["resolution"] for d in downloads if d.get("resolution")]
        if qs:
            quality = " / ".join(sorted(set(qs),
                key=lambda x: int(re.sub(r"\D", "", x) or 0), reverse=True))

    # title
    real_title = base.get("title", "")
    h1 = soup.select_one("h1.post-title, h1.entry-title, article h1, h1")
    if h1: real_title = h1.get_text(strip=True)
    if not real_title:
        og = soup.find("meta", property="og:title")
        if og: real_title = og.get("content", "").replace(" - HdMovieHub", "").strip()

    # cover
    real_cover = base.get("image", "")
    if not real_cover:
        og_img = soup.find("meta", property="og:image")
        if og_img: real_cover = og_img.get("content", "").strip()

    # date
    real_date = base.get("date", "")
    if not real_date:
        t = soup.select_one("time, .date-time span, .post-date")
        if t: real_date = t.get("datetime") or t.get_text(strip=True)

    return {
        "id":            slug(base["href"]),
        "href":          base["href"],
        "title":         real_title,
        "cover_image":   real_cover,
        "date_posted":   real_date,
        "page":          base.get("page"),
        "content_type":  content_type,
        "movie_name":    info.get("movie_name", ""),
        "release_year":  info.get("release_year", ""),
        "format":        info.get("format", ""),
        "size":          info.get("size", ""),
        "original_lang": info.get("original_lang", ""),
        "quality":       quality,
        "genres":        genres,
        "cast":          info.get("cast", ""),
        "imdb_rating":   info.get("imdb_rating", ""),
        "season":        info.get("season", ""),
        "episodes":      info.get("episodes", ""),
        "synopsis":      synopsis,
        "categories":    categories,
        "screenshots":   screenshots,
        "watch_online":  watch_online,
        "downloads":     downloads,
        "scraped_at":    datetime.now(timezone.utc).isoformat(),
        "detail_error":  None,
    }


# ── Worker ────────────────────────────────────────────────────────────────────

def process(base) -> dict:
    movie_id = slug(base["href"])
    if _stop_flag.is_set():
        return {**base, "id": movie_id, "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": "skipped_time_limit"}
    html = fetch(base["href"])
    if html is None:
        err = "fetch_failed" if not _stop_flag.is_set() else "skipped_time_limit"
        return {**base, "id": movie_id, "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": err}
    try:
        return parse(html, base)
    except Exception as e:
        return {**base, "id": movie_id, "scraped_at": datetime.now(timezone.utc).isoformat(),
                "detail_error": f"parse_error: {e}"}


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    global _start_time
    _start_time = time.monotonic()

    existing    = load_json(OUTPUT_FILE, [])
    result_map  = {r["id"]: r for r in existing if "id" in r}

    rescrape_list = load_json(RESCRAPE_FILE, None)

    if rescrape_list is not None:
        print(f"[INFO] TARGETED MODE — {len(rescrape_list)} URLs from {RESCRAPE_FILE}", flush=True)
        todo = [{"href": m["href"], "title": result_map.get(slug(m["href"]), {}).get("title",""),
                 "image": result_map.get(slug(m["href"]), {}).get("cover_image",""),
                 "date": result_map.get(slug(m["href"]), {}).get("date_posted",""),
                 "page": result_map.get(slug(m["href"]), {}).get("page", 0)}
                for m in rescrape_list]
        done_ids = set()  # always re-scrape all listed
    else:
        movies   = fetch_movies_list()
        cp_ids   = load_json(CHECKPOINT_FILE, [])
        done_ids = set(cp_ids) | set(result_map.keys())
        todo     = [m for m in movies if slug(m["href"]) not in done_ids]

    total = len(todo)
    print(f"[INFO] To scrape:   {total}", flush=True)
    print(f"[INFO] In output:   {len(result_map)}", flush=True)
    print(f"[INFO] Time limit:  {fmt_time(TIME_LIMIT_SECS)}", flush=True)

    if not todo:
        print("[INFO] Nothing to do.", flush=True)
        return

    completed = errors = skipped = 0
    save_lock = threading.Lock()
    pending   = [0]

    def do_save():
        save_json(OUTPUT_FILE,     list(result_map.values()))
        save_json(CHECKPOINT_FILE, list(done_ids))

    def watcher():
        while not _stop_flag.is_set():
            if elapsed() >= TIME_LIMIT_SECS - 60:
                print(f"\n[WARN] Time limit at {fmt_time(elapsed())} — draining …", flush=True)
                _stop_flag.set()
                return
            time.sleep(5)

    threading.Thread(target=watcher, daemon=True).start()

    LOG_EVERY = 50

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process, m): m for m in todo}
        for future in as_completed(futures):
            try:
                detail = future.result()
            except Exception as exc:
                base = futures[future]
                detail = {**base, "id": slug(base["href"]),
                          "scraped_at": datetime.now(timezone.utc).isoformat(),
                          "detail_error": f"exception: {exc}"}

            movie_id = detail.get("id", "")
            err      = detail.get("detail_error")

            with save_lock:
                result_map[movie_id] = detail
                done_ids.add(movie_id)
                if not err:                        completed += 1
                elif "skipped" in (err or ""):     skipped   += 1
                else:                              errors    += 1
                pending[0] += 1
                if pending[0] >= SAVE_EVERY:
                    do_save(); pending[0] = 0
                total_done = completed + errors + skipped
                if total_done % LOG_EVERY == 0 or total_done == total:
                    pct = total_done / total * 100
                    print(f"[{fmt_time(elapsed())}] {total_done}/{total} ({pct:.1f}%)  "
                          f"ok={completed} err={errors} skip={skipped} "
                          f"out={len(result_map)}", flush=True)

            if _stop_flag.is_set() and skipped > 0:
                break

    with save_lock:
        do_save()

    _stop_flag.set()
    print(f"\n[DONE] {fmt_time(elapsed())}  ok={completed} err={errors} skip={skipped}", flush=True)
    print(f"       Total in output: {len(result_map)}", flush=True)

    if rescrape_list is not None and skipped == 0:
        Path(RESCRAPE_FILE).unlink(missing_ok=True)
        print(f"[INFO] Removed {RESCRAPE_FILE} — targeted re-scrape complete", flush=True)


if __name__ == "__main__":
    main()
