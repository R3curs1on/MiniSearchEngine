"""
crawler.py — BFS Web Crawler (SQLite version)
No MySQL needed. DB is a single file: search_engine.db

Run: python crawler.py --seed https://docs.python.org/3/ --max 100
"""

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from collections import deque
import sqlite3
import time
import re
import argparse
import logging
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH          = os.path.join(os.path.dirname(__file__), "..", "search_engine.db")
POLITENESS_DELAY = 1.0
REQUEST_TIMEOUT  = 10
USER_AGENT       = "MiniSearchBot/1.0"

# ─── DB setup ─────────────────────────────────────────────────────────────────

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")   # faster concurrent writes
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def setup_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pages (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            url        TEXT NOT NULL UNIQUE,
            title      TEXT,
            body_text  TEXT,
            crawled_at TEXT DEFAULT (datetime('now'))
        );

        CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);

        CREATE TABLE IF NOT EXISTS crawl_log (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            url        TEXT,
            status     INTEGER,
            crawled_at TEXT DEFAULT (datetime('now'))
        );
    """)
    conn.commit()
    conn.close()

def url_exists(conn, url):
    row = conn.execute("SELECT 1 FROM pages WHERE url = ?", (url[:2000],)).fetchone()
    return row is not None

def save_page(conn, url, title, text):
    conn.execute(
        "INSERT OR IGNORE INTO pages (url, title, body_text) VALUES (?, ?, ?)",
        (url[:2000], (title or url)[:512], text)
    )
    conn.commit()

# ─── Robots.txt cache ─────────────────────────────────────────────────────────

_robots_cache = {}

def can_fetch(url):
    parsed = urlparse(url)
    base   = f"{parsed.scheme}://{parsed.netloc}"
    if base not in _robots_cache:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        _robots_cache[base] = rp
    return _robots_cache[base].can_fetch(USER_AGENT, url)

# ─── Crawl helpers ────────────────────────────────────────────────────────────

def extract_text(soup):
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()
    raw = soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", raw).strip()

def same_domain_links(base_url, soup):
    base_domain = urlparse(base_url).netloc
    links = []
    for a in soup.find_all("a", href=True):
        href   = urljoin(base_url, a["href"])
        parsed = urlparse(href)
        if parsed.scheme in ("http", "https") and parsed.netloc == base_domain:
            links.append(href.split("#")[0])
    return list(set(links))

# ─── Main crawl ───────────────────────────────────────────────────────────────

def crawl(seed_url, max_pages=200):
    setup_db()
    conn      = get_db()
    queue     = deque([seed_url])
    visited   = set()
    domain_ts = {}
    count     = 0

    while queue and count < max_pages:
        url = queue.popleft()
        if url in visited or not can_fetch(url):
            continue
        if url_exists(conn, url):
            visited.add(url)
            continue

        domain  = urlparse(url).netloc
        elapsed = time.time() - domain_ts.get(domain, 0)
        if elapsed < POLITENESS_DELAY:
            time.sleep(POLITENESS_DELAY - elapsed)

        try:
            log.info(f"[{count+1}/{max_pages}] {url}")
            resp = requests.get(
                url, timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                allow_redirects=True
            )
            domain_ts[domain] = time.time()

            if resp.status_code != 200:
                visited.add(url); continue
            if "text/html" not in resp.headers.get("Content-Type", ""):
                visited.add(url); continue

            soup  = BeautifulSoup(resp.text, "lxml")
            title = soup.title.string.strip() if soup.title else url
            text  = extract_text(soup)

            if len(text) < 100:
                visited.add(url); continue

            save_page(conn, url, title, text)
            visited.add(url)
            count += 1

            for link in same_domain_links(url, soup):
                if link not in visited:
                    queue.append(link)

        except Exception as e:
            log.warning(f"Failed {url}: {e}")
            visited.add(url)

    log.info(f"Done. Crawled {count} pages.")
    conn.close()

# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="Starting URL to crawl")
    parser.add_argument("--max",  type=int, default=200, help="Max pages to crawl")
    args = parser.parse_args()
    crawl(args.seed, args.max)
