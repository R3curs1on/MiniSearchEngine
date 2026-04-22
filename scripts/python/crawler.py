"""
crawler.py - BFS Web Crawler (MySQL version)
Run: python python/crawler.py --seed https://en.wikipedia.org/wiki/Main_Page --max 100
"""

import argparse
import hashlib
import logging
import os
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
DEFAULT_USER_AGENT = os.getenv("CRAWLER_USER_AGENT", "MiniIndexer/1.0 (+https://localhost/mini-search)")
USER_AGENT = DEFAULT_USER_AGENT


def get_db():
    return get_mysql_db()


def url_hash(url):
    return hashlib.sha256(url[:2000].encode("utf-8")).hexdigest()


def setup_db():
    conn = get_db()
    cursor = conn.cursor()

    create_table_pages_query =  """
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
    cursor.execute( create_table_pages_query )

    create_table_crawl_log_query =  """
        CREATE TABLE IF NOT EXISTS crawl_log (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            url VARCHAR(2000),
            status INT,
            crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    cursor.execute( create_table_crawl_log_query )

    create_table_crawl_edges_query =  """
        CREATE TABLE IF NOT EXISTS crawl_edges (
            source_url_hash CHAR(64) NOT NULL,
            target_url_hash CHAR(64) NOT NULL,
            PRIMARY KEY (source_url_hash, target_url_hash),
            KEY idx_edges_target (target_url_hash)
        )
        """
    cursor.execute( create_table_crawl_edges_query )

    conn.commit()
    cursor.close()
    conn.close()


def reset_crawl_tables():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM crawl_edges")
    cursor.execute("DELETE FROM crawl_log")
    cursor.execute("DELETE FROM pages")
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
    insert_query = """
        INSERT INTO pages (url, url_hash, title, body_text)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            id = LAST_INSERT_ID(id),
            url = VALUES(url),
            title = VALUES(title),
            body_text = VALUES(body_text),
            crawled_at = CURRENT_TIMESTAMP
        """
    cursor.execute(
        insert_query,
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
    insert_query = """
        INSERT IGNORE INTO crawl_edges (source_url_hash, target_url_hash)
        VALUES (%s, %s)
        """
    cursor.executemany(insert_query, rows)  
    conn.commit()
    cursor.close()


def log_crawl(conn, url, status):
    cursor = conn.cursor()
    insert_query = """
        INSERT INTO crawl_log (url, status)
        VALUES (%s, %s)
        """
    cursor.execute(
        insert_query,
        (url[:2000], status),
    )
    conn.commit()
    cursor.close()


_robots_cache = {}


def can_fetch(url, user_agent=USER_AGENT):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    cache_key = (base, user_agent)
    if cache_key not in _robots_cache:
        rp = RobotFileParser()
        robots_url = f"{base}/robots.txt"
        rp.set_url(robots_url)
        try:
            # Fetch robots.txt with the same UA used for page fetches.
            # Some hosts (including Wikipedia) may block generic urllib UAs.
            resp = requests.get(
                robots_url,
                timeout=REQUEST_TIMEOUT,
                headers={"User-Agent": user_agent},
                allow_redirects=True,
            )
            if resp.status_code in (401, 403):
                rp.disallow_all = True
            elif 400 <= resp.status_code < 500:
                rp.allow_all = True
            elif 200 <= resp.status_code < 400:
                rp.parse(resp.text.splitlines())
            else:
                log.warning("robots.txt returned status %s for %s", resp.status_code, base)
        except Exception as exc:
            log.warning("robots.txt check failed for %s: %s", base, exc)
        _robots_cache[cache_key] = rp
    return _robots_cache[cache_key].can_fetch(user_agent, url)


def is_wikipedia_url(url):
    host = urlparse(url).netloc.lower()
    return host == "wikipedia.org" or host.endswith(".wikipedia.org")


def wikipedia_article_url(url):
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return None
    if not is_wikipedia_url(url):
        return None
    if not parsed.path.startswith("/wiki/"):
        return None

    article = parsed.path[len("/wiki/"):]
    if not article or ":" in article:
        return None

    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def extract_text(soup, source_url=None):
    working_soup = soup

    if source_url and is_wikipedia_url(source_url):
        content = (
            soup.select_one("#mw-content-text")
            or soup.select_one("#bodyContent")
            or soup.select_one("main#content")
        )
        if content:
            working_soup = BeautifulSoup(str(content), "lxml")
        for selector in (
            "table.infobox",
            "table.vertical-navbox",
            ".navbox",
            ".metadata",
            ".mw-editsection",
            ".reference",
            ".reflist",
            ".thumb",
            ".mw-references-wrap",
        ):
            for node in working_soup.select(selector):
                node.decompose()

    for tag in working_soup(["script", "style", "noscript", "nav", "footer", "header"]):
        tag.decompose()
    raw = working_soup.get_text(separator=" ")
    return re.sub(r"\s+", " ", raw).strip()


def extract_title(soup, fallback, source_url=None):
    if source_url and is_wikipedia_url(source_url):
        heading = soup.select_one("#firstHeading")
        if heading:
            title = heading.get_text(" ", strip=True)
            if title:
                return title

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
        if parsed.scheme not in ("http", "https") or parsed.netloc != base_domain:
            continue

        if is_wikipedia_url(base_url):
            article_url = wikipedia_article_url(href)
            if article_url:
                links.append(article_url)
            continue

        links.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")
    return sorted(set(links))


def crawl(seed_url, max_pages=200, refresh_existing=False, fresh=False, user_agent=USER_AGENT):
    setup_db()
    if fresh:
        log.info("Clearing existing crawl corpus before starting new crawl.")
        reset_crawl_tables()
    conn = get_db()
    log.info("Using User-Agent: %s", user_agent)

    queue = deque([seed_url])
    visited = set()
    domain_ts = {}
    count = 0

    while queue and count < max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if not can_fetch(url, user_agent):
            log.info("Skipping %s because robots.txt disallows it for %s", url, user_agent)
            log_crawl(conn, url, 403)
            continue

        exists = url_exists(conn, url)
        if exists and not refresh_existing:
            log.info("Skipping already indexed URL: %s (use --refresh-existing or --fresh)", url)
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
                headers={"User-Agent": user_agent},
                allow_redirects=True,
            )
            domain_ts[domain] = time.time()
            log_crawl(conn, url, resp.status_code)

            if resp.status_code != 200:
                continue
            if "text/html" not in resp.headers.get("Content-Type", ""):
                continue

            encoding = resp.apparent_encoding or resp.encoding or "utf-8"
            html = resp.content.decode(encoding, errors="replace")
            soup = BeautifulSoup(html, "lxml")
            title = extract_title(soup, url, url)
            text = extract_text(soup, url)

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
        "--user-agent",
        default=USER_AGENT,
        help="User-Agent to send for robots.txt checks and page fetches",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Delete existing pages/crawl history first. Use this when switching corpora.",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-fetch pages that already exist so titles/body/edges can be refreshed",
    )
    args = parser.parse_args()
    crawl(args.seed, args.max, args.refresh_existing, args.fresh, args.user_agent)
