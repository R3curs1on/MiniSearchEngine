"""
crawler.py - BFS Web Crawler (MySQL version)

Run: python crawler.py --seed https://docs.python.org/3/ --max 100
"""

import argparse
import hashlib
import logging
import re
import time
from collections import deque
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from db import get_db as get_mysql_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

POLITENESS_DELAY = 1.0
REQUEST_TIMEOUT = 10
USER_AGENT = "MiniSearchBot/1.0"


def get_db():
    return get_mysql_db()


def url_hash(url):
    return hashlib.sha256(url[:2000].encode("utf-8")).hexdigest()


def setup_db():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pages (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            url VARCHAR(2000) NOT NULL,
            url_hash CHAR(64) NOT NULL,
            title VARCHAR(512),
            body_text MEDIUMTEXT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_pages_url_hash (url_hash),
            KEY idx_pages_url (url(255))
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_log (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            url VARCHAR(2000),
            status INT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS crawl_edges (
            source_url_hash CHAR(64) NOT NULL,
            target_url_hash CHAR(64) NOT NULL,
            PRIMARY KEY (source_url_hash, target_url_hash),
            KEY idx_edges_target (target_url_hash)
        )
        """
    )
    conn.commit()
    cursor.close()
    conn.close()


def url_exists(conn, url):
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM pages WHERE url_hash = %s", (url_hash(url),))
    row = cursor.fetchone()
    cursor.close()
    return row is not None


def save_page(conn, url, title, text):
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO pages (url, url_hash, title, body_text)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            url = VALUES(url),
            title = VALUES(title),
            body_text = VALUES(body_text),
            crawled_at = CURRENT_TIMESTAMP
        """,
        (url[:2000], url_hash(url), (title or url)[:512], text),
    )
    conn.commit()
    page_id = cursor.lastrowid
    cursor.close()
    return page_id


def save_edges(conn, source_url, links):
    if not links:
        return
    source_hash = url_hash(source_url)
    rows = [(source_hash, url_hash(link)) for link in links]
    cursor = conn.cursor()
    cursor.executemany(
        """
        INSERT IGNORE INTO crawl_edges (source_url_hash, target_url_hash)
        VALUES (%s, %s)
        """,
        rows,
    )
    conn.commit()
    cursor.close()


def log_crawl(conn, url, status):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO crawl_log (url, status) VALUES (%s, %s)",
        (url[:2000], status),
    )
    conn.commit()
    cursor.close()


_robots_cache = {}


def can_fetch(url):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base not in _robots_cache:
        rp = RobotFileParser()
        rp.set_url(f"{base}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        _robots_cache[base] = rp
    return _robots_cache[base].can_fetch(USER_AGENT, url)


def extract_text(soup):
    for tag in soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()
    raw = soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", raw).strip()


def extract_title(soup, fallback):
    if not soup.title:
        return fallback
    title = soup.title.get_text(" ", strip=True)
    return title or fallback


def same_domain_links(base_url, soup):
    base_domain = urlparse(base_url).netloc
    links = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        parsed = urlparse(href)
        if parsed.scheme in ("http", "https") and parsed.netloc == base_domain:
            links.append(href.split("#")[0])
    return list(set(links))


def crawl(seed_url, max_pages=200, refresh_existing=False):
    setup_db()
    conn = get_db()

    queue = deque([seed_url])
    visited = set()
    domain_ts = {}
    count = 0

    while queue and count < max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not can_fetch(url):
            log_crawl(conn, url, 403)
            continue

        exists = url_exists(conn, url)
        if exists and not refresh_existing:
            continue

        domain = urlparse(url).netloc
        elapsed = time.time() - domain_ts.get(domain, 0)
        if elapsed < POLITENESS_DELAY:
            time.sleep(POLITENESS_DELAY - elapsed)

        try:
            log.info("[%s/%s] %s", count + 1, max_pages, url)
            resp = requests.get(
                url,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": USER_AGENT},
                allow_redirects=True,
            )
            domain_ts[domain] = time.time()
            log_crawl(conn, url, resp.status_code)

            if resp.status_code != 200:
                continue
            if "text/html" not in resp.headers.get("Content-Type", ""):
                continue

            # Prefer detected/apparent encoding over requests' text/* fallback.
            encoding = resp.apparent_encoding or resp.encoding or "utf-8"
            html = resp.content.decode(encoding, errors="replace")
            soup = BeautifulSoup(html, "lxml")
            title = extract_title(soup, url)
            text = extract_text(soup)

            if len(text) < 100:
                continue

            save_page(conn, url, title, text)
            links = same_domain_links(url, soup)
            save_edges(conn, url, links)
            count += 1

            for link in links:
                if link not in visited:
                    queue.append(link)
        except Exception as exc:
            log.warning("Failed %s: %s", url, exc)
            log_crawl(conn, url, 0)

    log.info("Done. Crawled %s pages.", count)
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="Starting URL to crawl")
    parser.add_argument("--max", type=int, default=200, help="Max pages to crawl")
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-fetch pages that already exist so titles/body/edges can be refreshed",
    )
    args = parser.parse_args()
    crawl(args.seed, args.max, args.refresh_existing)
