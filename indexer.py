"""
indexer.py — TF-IDF Inverted Index Builder (MySQL version)
Reads crawled pages from MySQL and builds the inverted index in-place.

Run: python indexer.py
"""

import math
import json
import re
import logging
from collections import defaultdict

from db import get_db as get_mysql_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

STOPWORDS = set("""
a about above after again against all am an and any are as at be because been
before being below between both but by did do does doing down during each few
for from get got have he her here him his how i if in into is it its let me
more most my no nor not of off on once only or other our out over own same she
should so some such than that the their them then there these they this those
through to too under until up very was we were what when where which while who
whom why will with you your
""".split())

# ─── DB connection ────────────────────────────────────────────────────────────

def get_db():
    return get_mysql_db()

def setup_index_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS terms (
            id BIGINT PRIMARY KEY AUTO_INCREMENT,
            term VARCHAR(255) NOT NULL,
            doc_freq INT DEFAULT 0,
            UNIQUE KEY uq_terms_term (term),
            KEY idx_terms_term (term)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS postings (
            term_id BIGINT NOT NULL,
            page_id BIGINT NOT NULL,
            tf_idf DOUBLE NOT NULL,
            tf DOUBLE NOT NULL,
            positions JSON,
            PRIMARY KEY (term_id, page_id),
            KEY idx_postings_term (term_id),
            KEY idx_postings_score (term_id, tf_idf)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS page_rank (
            page_id BIGINT PRIMARY KEY,
            score DOUBLE DEFAULT 1.0,
            word_count INT DEFAULT 0
        )
    """)
    conn.commit()
    cursor.close()

# ─── Text processing ──────────────────────────────────────────────────────────

def tokenize(text):
    tokens = re.findall(r"[a-z]{2,}", text.lower())
    return [t for t in tokens if t not in STOPWORDS]

def compute_tf(tokens):
    freq  = defaultdict(int)
    for t in tokens:
        freq[t] += 1
    total = len(tokens) or 1
    return {term: count / total for term, count in freq.items()}

def get_positions(tokens, term):
    return [i for i, t in enumerate(tokens) if t == term][:50]

# ─── Indexer ──────────────────────────────────────────────────────────────────

def build_index():
    conn = get_db()
    setup_index_tables(conn)
    cursor = conn.cursor(dictionary=True)

    # Clear old index (re-index from scratch)
    cursor.execute("DELETE FROM postings")
    cursor.execute("DELETE FROM terms")
    cursor.execute("DELETE FROM page_rank")
    conn.commit()

    log.info("Loading pages...")
    cursor.execute("SELECT id, title, body_text FROM pages")
    pages = cursor.fetchall()
    N     = len(pages)
    log.info(f"Total pages: {N}")

    if N == 0:
        log.warning("No pages found. Run crawler.py first.")
        conn.close(); return

    # ── Pass 1: tokenize + build document-frequency map ──────────────────────
    log.info("Pass 1: Tokenizing all pages...")
    doc_tokens = {}          # page_id → [tokens]
    df_map     = defaultdict(set)   # term → {page_ids}

    for page in pages:
        pid    = page["id"]
        text   = (page["title"] or "") + " " + (page["body_text"] or "")
        tokens = tokenize(text)
        doc_tokens[pid] = tokens
        for term in set(tokens):
            df_map[term].add(pid)

    # ── Insert terms ──────────────────────────────────────────────────────────
    log.info(f"Inserting {len(df_map)} unique terms...")
    insert_terms = conn.cursor()
    insert_terms.executemany(
        """
        INSERT INTO terms (term, doc_freq)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE doc_freq = VALUES(doc_freq)
        """,
        [(term, len(pages_set)) for term, pages_set in df_map.items()]
    )
    conn.commit()
    insert_terms.close()

    # Load term id → id mapping
    cursor.execute("SELECT id, term FROM terms")
    term_id_map = {
        row["term"]: row["id"]
        for row in cursor.fetchall()
    }

    # ── Pass 2: compute TF-IDF and write postings ─────────────────────────────
    log.info("Pass 2: Computing TF-IDF...")
    postings_batch = []
    page_rank_batch = []
    BATCH_SIZE = 5000
    written = 0

    for page in pages:
        pid    = page["id"]
        tokens = doc_tokens[pid]
        if not tokens:
            continue

        tf_map     = compute_tf(tokens)
        word_count = len(tokens)
        pr_score   = math.log(word_count + 1)
        page_rank_batch.append((pid, pr_score, word_count))

        for term, tf in tf_map.items():
            tid = term_id_map.get(term)
            if not tid: continue
            df    = len(df_map[term])
            idf   = math.log(N / df) if df > 0 else 0
            score = tf * idf
            pos   = json.dumps(get_positions(tokens, term))
            postings_batch.append((tid, pid, score, tf, pos))
            written += 1

            if len(postings_batch) >= BATCH_SIZE:
                postings_cursor = conn.cursor()
                postings_cursor.executemany(
                    """
                    INSERT INTO postings (term_id, page_id, tf_idf, tf, positions)
                    VALUES (%s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        tf_idf = VALUES(tf_idf),
                        tf = VALUES(tf),
                        positions = VALUES(positions)
                    """,
                    postings_batch
                )
                conn.commit()
                postings_cursor.close()
                postings_batch.clear()
                log.info(f"  Written {written} postings...")

    # Flush remaining
    if postings_batch:
        postings_cursor = conn.cursor()
        postings_cursor.executemany(
            """
            INSERT INTO postings (term_id, page_id, tf_idf, tf, positions)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                tf_idf = VALUES(tf_idf),
                tf = VALUES(tf),
                positions = VALUES(positions)
            """,
            postings_batch
        )
        postings_cursor.close()
    if page_rank_batch:
        page_rank_cursor = conn.cursor()
        page_rank_cursor.executemany(
            """
            INSERT INTO page_rank (page_id, score, word_count)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                score = VALUES(score),
                word_count = VALUES(word_count)
            """,
            page_rank_batch
        )
        page_rank_cursor.close()
    conn.commit()

    log.info(f"Indexing complete! Total postings: {written}")
    cursor.close()
    conn.close()

# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    build_index()
