"""
crawler.py — BFS Web Crawler (MySQL version)

Run: python crawler.py --seed https://docs.python.org/3/ --max 100
"""

from socket import close

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser
from collections import deque
import time
import re
import argparse
import logging
import hashlib

from db import get_db as get_mysql_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

POLITENESS_DELAY = 1.0                  # delay in seconds between requests to the same domain; this is important to avoid overwhelming websites with too many requests in a short period of time, which can lead to being blocked or causing performance issues for the website; setting a politeness delay helps ensure that our crawler behaves responsibly and respects the resources of the websites it visits
REQUEST_TIMEOUT  = 10                   # timeout for HTTP requests in seconds; this helps prevent our crawler from hanging indefinitely on slow or unresponsive websites, ensuring that it can move on to other URLs in a timely manner; it's important to set a reasonable timeout to balance between giving websites enough time to respond and keeping the crawler efficient
USER_AGENT       = "MiniSearchBot/1.0"  # identify our crawler with a custom user agent string; this is important for ethical crawling and to avoid being blocked by websites that disallow generic user agents; it also helps website administrators understand who is crawling their site when they check their server logs

# ─── DB setup ─────────────────────────────────────────────────────────────────

def get_db():
    return get_mysql_db()

def url_hash(url):
    return hashlib.sha256(url[:2000].encode("utf-8")).hexdigest()   #sha256 hash of url 

def setup_db():
    conn = get_db()
    cursor = conn.cursor()  # create a cursor to execute SQL commands using python's DB-API
    ''' 
    MySQL schema:
    table name : pages
    - id (BIGINT, PK, AUTO_INCREMENT)           // Unique page (webpage) id
    - url (VARCHAR(2000), NOT NULL)             // 2000 is max URL length in practice
    - url_hash (CHAR(64), NOT NULL, UNIQUE)     // SHA-256 hash of URL for fast existence checks
    - title (VARCHAR(512))                      // Page title (truncated to 512 chars)
    - body_text (MEDIUMTEXT)                    // Main text content (up to 16MB)
    - crawled_at (TIMESTAMP, default CURRENT_TIMESTAMP)  //timestamp of when the page was crawled ;used for logging and freshness checks

    indexes:                                    // used to speed up queries
    - UNIQUE KEY on url_hash for fast existence checks  // ensures we don't store duplicates by using the hash of the URL;
    - KEY on url(255) for faster lookups (optional)     // indexing the first 255 chars of the URL can speed up queries that filter by URL, but it's optional and depends on your query patterns

    '''
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

    '''
    MySQL schema for crawl_log (used for logging crawl attempts and statuses):
    table name : crawl_log
    - id (BIGINT, PK, AUTO_INCREMENT)           // Unique log entry id
    - url (VARCHAR(2000))                       // URL that was attempted to crawl
    - status (INT)                             // HTTP status code or custom code for crawl result
    - crawled_at (TIMESTAMP, default CURRENT_TIMESTAMP)  // Timestamp of when the crawl attempt was made
    '''
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

    conn.commit()   # commit the changes to the database
    cursor.close()  # close the cursor to free up resources
    conn.close()    # close the connection to free up resources

def url_exists(conn, url):  # check if a URL already exists in the database using its hash
    cursor = conn.cursor()  
    # if we found any row with the same url_hash, it means the URL already exists in the database, so we return True; otherwise, we return False
    cursor.execute(
        "SELECT 1 FROM pages WHERE url_hash = %s",   #syntax for parameterized query in MySQL; %s is a placeholder for the url_hash value that will be safely substituted by the database driver to prevent SQL injection
        # (url_hash(url),)    # we pass the url_hash as a tuple (with a comma) to match the expected parameter format for the execute method ie execute(sql, params) here
        url_hash(url)  # we can pass the url_hash directly without wrapping it in a tuple since it's a single value; the database driver will handle it correctly as a parameter
    )
    row = cursor.fetchone()   # fetchone() retrieves the next row of a query result set; it returns a single sequence, or None if no more rows are available; in this case there cant be more than one row having same url hash due to the UNIQUE constraint on url_hash column, so fetchone() is sufficient to check for existence
    cursor.close()         # close the cursor to free up resources
    return row is not None   

def save_page(conn, url, title, text):    # when crawled a page ; save data to db ; data is (url, title, text (body text))
    cursor = conn.cursor()
    # query for inserting is "insert into tableName (col1,col2,...) values (%s, %s, ...)" ; 
    # we use ON "DUPLICATE KEY UPDATE" to handle the case where a page with the same url_hash already exists in the database;
    # if a duplicate key (url_hash) is found, it will update the existing record with the new url, title, and body_text values instead of inserting a new row; 
    # this ensures that we don't have duplicate entries for the same URL and allows us to keep the page information up-to-date if it changes
    cursor.execute(
        """
        INSERT INTO pages (url, url_hash, title, body_text)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            url = VALUES(url),
            title = VALUES(title),
            body_text = VALUES(body_text)
        """,
        (url[:2000], url_hash(url), (title or url)[:512], text)
    )
    conn.commit()   # commit the transaction to save changes to the database
    cursor.close()

def log_crawl(conn, url, status):   #logging crawl attempts and their statuses to the crawl_log table in the database;
                                    # this function takes a database connection, the URL that was attempted to be crawled, and the status code (e.g., HTTP status code or custom code) representing the result of the crawl attempt; 
                                    # it inserts a new record into the crawl_log table with the provided URL and status, along with a timestamp of when the crawl attempt was made (handled by the default value of crawled_at column)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO crawl_log (url, status) VALUES (%s, %s)",
        (url[:2000], status)
    )
    conn.commit()
    cursor.close()

# ─── Robots.txt cache ─────────────────────────────────────────────────────────
_robots_cache = {}  # cache for RobotFileParser instances keyed by base URL (scheme + netloc)

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

def extract_title(soup, fallback):   # extrack title from html pages 
    if not soup.title:               # if no title found we use fallback which is url itself ; this ensures that we always have a meaningful title for the page, even if the HTML doesn't contain a <title> tag, which can be important for indexing and displaying search results in our mini search engine
        return fallback

    title = soup.title.get_text(" ", strip=True)     
    return title or fallback

def same_domain_links(base_url, soup):   
    # this is similar to for( neighbour : graph[currNode]) in BFS 
    # we extract all the links from the page and filter them to include only those that belong to the same domain as the base URL;  this helps us stay within the same website during our crawl and avoid going off-site, which is important for focused crawling and respecting website boundaries
    base_domain = urlparse(base_url).netloc  # netlock is domain+port
    links = []
    for a in soup.find_all("a", href=True):   # find_all gives all <a> tags with href attribute; we iterate over these tags to extract the href values, which represent the links on the page; this allows us to discover new URLs to crawl that are linked from the current page
        href   = urljoin(base_url, a["href"])
        parsed = urlparse(href)
        if parsed.scheme in ("http", "https") and parsed.netloc == base_domain:
            links.append(href.split("#")[0])
    return list(set(links))

# ─── Main crawl ───────────────────────────────────────────────────────────────

def crawl(seed_url, max_pages=200):  
    # seed_url is the starting point of the crawl, and max_pages limits the number of pages to crawl to prevent infinite crawling;
    # this function implements a breadth-first search (BFS) approach to crawl web pages starting from the seed URL, while respecting robots.txt rules and logging crawl attempts and results to a MySQL database

    setup_db()  
    root = [seed_url]
    conn      = get_db()
    queue     = deque(root)  # double ended queue to perform BFS; we start with seed_url as root of our crawl;
    visited   = set()              # set to keep track of visited url ; to avoud cycles and redundant crawling; we add a URL to visited set once we have processed it (regardless of success or failure) to ensure we don't attempt to crawl the same URL multiple times, which can lead to infinite loops and inefficient crawling
    domain_ts = {}                 # dictionary to track last access time for each domain; this is used to enforce politeness by ensuring we wait at least POLITENESS_DELAY seconds between requests to the same domain; we update the timestamp for a domain each time we make a request to it, and check the elapsed time before making another request to the same domain
    count     = 0                  # cnt of how many pages we have successfully crawled and saved to the database; we increment this count only when we successfully crawl a page (i.e., get a 200 response and save it to the database); this count is used to enforce the max_pages limit for the crawl


    while queue and count < max_pages:  #while !q.empty() && max_pages not reached
        url = queue.popleft()           # q.popFront()
        if url in visited:              # to avoid cycle  and redundant crawling;
            continue

        if not can_fetch(url):         # check if that node is accessible ie if we are allowed to crawl that url (according to robots.txt)
            visited.add(url)           
            log_crawl(conn, url, 403)  # mark log entry with 403 status code to indicate that the crawl was forbidden by robots.txt rules; this helps us keep track of which URLs were not crawled due to access restrictions, and can be useful for analysis and debugging of our crawling process
            continue

        if visited[url] or url_exists(conn, url):      # already crawled or already in db
            visited.add(url)
            continue

        domain  = urlparse(url).netloc                # extract the domain (netloc) from the URL to check for politeness delay; 
                                                      # this allows us to track the last access time for each domain and ensure that we wait an appropriate amount of time between requests to the same domain,
                                                      #  which is important for responsible crawling and to avoid overwhelming websites with too many requests in a short period of time
        elapsed = time.time() - domain_ts.get(domain, 0)   # currtime - lastAccessTime ; 
        if elapsed < POLITENESS_DELAY:                  # if we have accessed this domain recently (within the politeness delay), we wait for the remaining time before making another request to the same domain;
            time.sleep(POLITENESS_DELAY - elapsed)

        try:                
            log.info(f"[{count+1}/{max_pages}] {url}")   # log currURL
            resp = requests.get(                         # actually make http get req to fetch page content;
                url, timeout=REQUEST_TIMEOUT,          
                headers={"User-Agent": USER_AGENT},      # applying the custom user agent string to identify our crawler; ie miniseachbot/1.0; 
                allow_redirects=True                     # allows the request to follow HTTP redirects (e.g., 301, 302) automatically, which is important for crawling as many pages as possible and handling cases where URLs may redirect to other URLs; 
            )
            domain_ts[domain] = time.time()             # store last access time for this domain for politeness delay tracking; 
            log_crawl(conn, url, resp.status_code)      # log the crawl

            if resp.status_code != 200:                 # if we didnt get a successful response, we mark the URL as visited to avoid retrying it,
                visited.add(url); continue
            if "text/html" not in resp.headers.get("Content-Type", ""): # if content type is not html ;
                visited.add(url); continue

            soup  = BeautifulSoup(resp.text, "lxml")    # if page is crawl succesfully and is html ; we parse html 
            title = extract_title(soup, url)            # extract title from html page and if no title found use uel as fallback
            text  = extract_text(soup)                  # parse the main text content from the HTML page by removing script, style, and other non-content elements, and normalizing whitespace; this gives us the body text of the page that we want to save in our database for indexing and search purposes

            if len(text) < 100:                         # if the extracted text content is too short (less than 100 characters), we consider it not meaningful enough to save in our database, and we skip saving this page; this helps us avoid cluttering our database with pages that don't have substantial content, which can improve the quality of our search results and reduce storage of low-value pages
                visited.add(url); continue

            save_page(conn, url, title, text)           # save craw;ed page to db
            visited.add(url)   
            count += 1

            for link in same_domain_links(url, soup):   # for(neighbour : graph[currNode]) in BFS ; we extract all the links from the page that belong to the same domain and add them to the queue for crawling;
                if link not in visited:
                    queue.append(link)

        except Exception as e:
            log.warning(f"Failed {url}: {e}")
            log_crawl(conn, url, 0)
            visited.add(url)

    log.info(f"Done. Crawled {count} pages.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", required=True, help="Starting URL to crawl")
    parser.add_argument("--max",  type=int, default=200, help="Max pages to crawl")
    args = parser.parse_args()
    crawl(args.seed, args.max)
