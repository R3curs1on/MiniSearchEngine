"""
indexer.py — TF-IDF Inverted Index Builder (SQLite version)
Reads crawled pages from search_engine.db, builds the inverted index in-place.

Run: python indexer.py
"""

import sqlite3
import math
import json
import re
import logging
import os
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "search_engine.db")

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
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")   # 64MB cache
    return conn

def setup_index_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS terms (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            term     TEXT NOT NULL UNIQUE,
            doc_freq INTEGER DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_terms_term ON terms(term);

        CREATE TABLE IF NOT EXISTS postings (
            term_id   INTEGER NOT NULL,
            page_id   INTEGER NOT NULL,
            tf_idf    REAL NOT NULL,
            tf        REAL NOT NULL,
            positions TEXT,
            PRIMARY KEY (term_id, page_id)
        );

        CREATE INDEX IF NOT EXISTS idx_postings_term  ON postings(term_id);
        CREATE INDEX IF NOT EXISTS idx_postings_score ON postings(term_id, tf_idf DESC);

        CREATE TABLE IF NOT EXISTS page_rank (
            page_id    INTEGER PRIMARY KEY,
            score      REAL DEFAULT 1.0,
            word_count INTEGER DEFAULT 0
        );
    """)
    conn.commit()

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

    # Clear old index (re-index from scratch)
    conn.executescript("DELETE FROM postings; DELETE FROM terms; DELETE FROM page_rank;")
    conn.commit()

    log.info("Loading pages...")
    pages = conn.execute("SELECT id, title, body_text FROM pages").fetchall()
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
    conn.executemany(
        "INSERT OR IGNORE INTO terms (term, doc_freq) VALUES (?, ?)",
        [(term, len(pages_set)) for term, pages_set in df_map.items()]
    )
    conn.commit()

    # Load term id → id mapping
    term_id_map = {
        row["term"]: row["id"]
        for row in conn.execute("SELECT id, term FROM terms").fetchall()
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
                conn.executemany(
                    "INSERT OR REPLACE INTO postings (term_id, page_id, tf_idf, tf, positions) VALUES (?,?,?,?,?)",
                    postings_batch
                )
                conn.commit()
                postings_batch.clear()
                log.info(f"  Written {written} postings...")

    # Flush remaining
    if postings_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO postings (term_id, page_id, tf_idf, tf, positions) VALUES (?,?,?,?,?)",
            postings_batch
        )
    if page_rank_batch:
        conn.executemany(
            "INSERT OR REPLACE INTO page_rank (page_id, score, word_count) VALUES (?,?,?)",
            page_rank_batch
        )
    conn.commit()

    log.info(f"Indexing complete! Total postings: {written}")
    conn.close()

# ─── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    build_index()
