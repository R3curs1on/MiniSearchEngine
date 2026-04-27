"""Build a BM25 inverted index, PageRank scores, and index metadata in MySQL."""

import json
import logging
import math
import re
import time
from collections import defaultdict
from typing import Any

from db import get_db as get_mysql_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

STOPWORDS = set(
    """
    a about above after again against all am an and any are as at be because been
    before being below between both but by did do does doing down during each few
    for from get got have he her here him his how i if in into is it its let me
    more most my no nor not of off on once only or other our out over own same she
    should so some such than that the their them then there these they this those
    through to too under until up very was we were what when where which while who
    whom why will with you your
    """.split()
)
BM25_K1 = 1.2
BM25_B = 0.75
TITLE_BOOST = 2.5
PAGE_RANK_WEIGHT = 0.35
COVERAGE_WEIGHT = 0.20
PHRASE_WEIGHT = 0.90
PROXIMITY_WEIGHT = 0.45
PAGERANK_DAMPING = 0.85
PAGERANK_ITERATIONS = 20
BATCH_SIZE = 5000


def get_db() -> Any:
    """Return a configured MySQL connection for indexing operations."""
    return get_mysql_db()


def setup_index_tables(conn: Any) -> None:
    """Rebuild the index tables from scratch before each indexing run."""
    cursor = conn.cursor()
    try:
        cursor.execute("DROP TABLE IF EXISTS postings")
        cursor.execute("DROP TABLE IF EXISTS terms")
        cursor.execute("DROP TABLE IF EXISTS page_rank")
        cursor.execute("DROP TABLE IF EXISTS index_meta")

        cursor.execute(
            """
            CREATE TABLE terms (
                id BIGINT PRIMARY KEY AUTO_INCREMENT,
                term VARCHAR(255) NOT NULL,
                doc_freq INT NOT NULL DEFAULT 0,
                idf DOUBLE NOT NULL DEFAULT 0,
                UNIQUE KEY uq_terms_term (term),
                KEY idx_terms_term (term)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE postings (
                term_id BIGINT NOT NULL,
                page_id BIGINT NOT NULL,
                tf_title DOUBLE NOT NULL DEFAULT 0,
                tf_body DOUBLE NOT NULL DEFAULT 0,
                freq_title INT NOT NULL DEFAULT 0,
                freq_body INT NOT NULL DEFAULT 0,
                positions JSON,
                PRIMARY KEY (term_id, page_id),
                KEY idx_postings_term (term_id),
                KEY idx_postings_page (page_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE page_rank (
                page_id BIGINT PRIMARY KEY,
                score DOUBLE NOT NULL DEFAULT 0,
                word_count INT NOT NULL DEFAULT 0,
                in_links INT NOT NULL DEFAULT 0,
                out_links INT NOT NULL DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE index_meta (
                key_name VARCHAR(64) PRIMARY KEY,
                value_num DOUBLE NULL,
                value_text VARCHAR(255) NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()
    finally:
        cursor.close()


def tokenize(text: str) -> dict:
    """Convert raw text into term -> positions mappings for indexing."""
    words = re.findall(r"[a-z]{2,}", (text or "").lower())
    token_map = {}
    for idx, term in enumerate(words):
        if term in STOPWORDS:
            continue
        positions = token_map.setdefault(term, [])
        if len(positions) < 50:
            positions.append(idx)
    return token_map


def term_counts(token_map: dict) -> dict:
    """Convert a token map into simple term frequencies."""
    return {term: len(pos) for term, pos in token_map.items()}


def fetch_pages(conn: Any) -> list:
    """Fetch all pages that should be included in the index build."""
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute("SELECT id, title, body_text FROM pages")
        return cursor.fetchall()
    finally:
        cursor.close()


def build_document_maps(pages: list) -> tuple[dict, dict, defaultdict]:
    """Tokenize each page and compute the statistics needed for BM25."""
    doc_maps = {}
    doc_lens = {}
    df_map = defaultdict(set)

    for page in pages:
        pid = page["id"]
        title_map = tokenize(page.get("title") or "")
        body_map = tokenize(page.get("body_text") or "")
        title_counts = term_counts(title_map)
        body_counts = term_counts(body_map)
        all_terms = set(title_counts) | set(body_counts)

        doc_maps[pid] = {
            "title_map": title_map,
            "body_map": body_map,
            "title_counts": title_counts,
            "body_counts": body_counts,
            "terms": all_terms,
        }

        doc_lens[pid] = sum(title_counts.values()) + sum(body_counts.values())
        for term in all_terms:
            df_map[term].add(pid)

    return doc_maps, doc_lens, df_map


def insert_terms(conn: Any, df_map: defaultdict, page_count: int) -> dict:
    """Insert term statistics and return a term -> id lookup map."""
    term_rows = []
    for term, pages_set in df_map.items():
        df = len(pages_set)
        idf = math.log(1.0 + ((page_count - df + 0.5) / (df + 0.5)))
        term_rows.append((term, df, idf))

    cursor = conn.cursor()
    try:
        cursor.executemany(
            "INSERT INTO terms (term, doc_freq, idf) VALUES (%s, %s, %s)",
            term_rows,
        )
        conn.commit()
        cursor.execute("SELECT id, term FROM terms")
        return {row["term"]: row["id"] for row in cursor.fetchall()}
    finally:
        cursor.close()


def insert_postings(
    conn: Any,
    pages: list,
    doc_maps: dict,
    term_id_map: dict,
) -> int:
    """Write BM25 postings in batches and return the number of rows inserted."""
    postings_batch = []
    written_postings = 0
    cursor = conn.cursor()
    try:
        for page in pages:
            pid = page["id"]
            maps = doc_maps[pid]
            title_counts = maps["title_counts"]
            body_counts = maps["body_counts"]

            for term in maps["terms"]:
                term_id = term_id_map.get(term)
                if not term_id:
                    continue

                freq_title = title_counts.get(term, 0)
                freq_body = body_counts.get(term, 0)
                postings_batch.append(
                    (
                        term_id,
                        pid,
                        float(freq_title),
                        float(freq_body),
                        freq_title,
                        freq_body,
                        json.dumps(
                            {
                                "title": maps["title_map"].get(term, [])[:20],
                                "body": maps["body_map"].get(term, [])[:50],
                            }
                        ),
                    )
                )
                written_postings += 1

                if len(postings_batch) >= BATCH_SIZE:
                    cursor.executemany(
                        """
                        INSERT INTO postings
                            (term_id, page_id, tf_title, tf_body, freq_title, freq_body, positions)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """,
                        postings_batch,
                    )
                    conn.commit()
                    postings_batch.clear()

        if postings_batch:
            cursor.executemany(
                """
                INSERT INTO postings
                    (term_id, page_id, tf_title, tf_body, freq_title, freq_body, positions)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                postings_batch,
            )
            conn.commit()
        return written_postings
    finally:
        cursor.close()


def compute_link_page_rank(
    conn: Any, page_ids: list
) -> tuple[dict, dict, dict]:
    """Compute normalized PageRank scores plus in/out link counts for the pages."""
    if not page_ids:
        return {}, {}, {}

    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(
            """
            SELECT s.id AS source_id, t.id AS target_id
            FROM crawl_edges e
            JOIN pages s ON s.url_hash = e.source_url_hash
            JOIN pages t ON t.url_hash = e.target_url_hash
            WHERE s.id <> t.id
            """
        )
        edges = cursor.fetchall()
    except Exception:
        log.warning("crawl_edges table missing or unreadable; falling back to zero page_rank.")
        zeros = {pid: 0.0 for pid in page_ids}
        counts = {pid: 0 for pid in page_ids}
        return zeros, counts, counts
    finally:
        cursor.close()

    adjacency = {pid: set() for pid in page_ids}
    incoming = {pid: 0 for pid in page_ids}

    for row in edges:
        source = row["source_id"]
        target = row["target_id"]
        if source in adjacency and target in adjacency and target not in adjacency[source]:
            adjacency[source].add(target)
            incoming[target] += 1

    outgoing = {pid: len(targets) for pid, targets in adjacency.items()}
    n = len(page_ids)
    damping = PAGERANK_DAMPING
    pr = {pid: 1.0 / n for pid in page_ids}

    for _ in range(PAGERANK_ITERATIONS):
        base = (1.0 - damping) / n
        next_pr = {pid: base for pid in page_ids}
        dangling_mass = sum(pr[pid] for pid in page_ids if outgoing[pid] == 0)
        dangling_share = damping * dangling_mass / n

        for pid in page_ids:
            next_pr[pid] += dangling_share

        for source in page_ids:
            out_degree = outgoing[source]
            if out_degree == 0:
                continue
            share = damping * pr[source] / out_degree
            for target in adjacency[source]:
                next_pr[target] += share
        pr = next_pr

    min_pr = min(pr.values()) if pr else 0.0
    max_pr = max(pr.values()) if pr else 0.0
    normalized = (
        {pid: (score - min_pr) / (max_pr - min_pr) for pid, score in pr.items()}
        if max_pr > min_pr
        else {pid: 0.0 for pid in page_ids}
    )

    return normalized, incoming, outgoing


def write_page_rank(
    conn: Any,
    page_ids: list,
    doc_lens: dict,
) -> None:
    """Persist PageRank and page structure statistics."""
    pr_scores, incoming, outgoing = compute_link_page_rank(conn, page_ids)
    page_rank_rows = [
        (
            pid,
            pr_scores.get(pid, 0.0),
            int(doc_lens.get(pid, 0)),
            int(incoming.get(pid, 0)),
            int(outgoing.get(pid, 0)),
        )
        for pid in page_ids
    ]

    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO page_rank (page_id, score, word_count, in_links, out_links)
            VALUES (%s, %s, %s, %s, %s)
            """,
            page_rank_rows,
        )
        conn.commit()
    finally:
        cursor.close()


def upsert_index_meta(conn, rows) -> None:
    """Store the current ranking configuration and corpus metadata."""
    cursor = conn.cursor()
    try:
        cursor.executemany(
            """
            INSERT INTO index_meta (key_name, value_num, value_text)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                value_num = VALUES(value_num),
                value_text = VALUES(value_text)
            """,
            rows,
        )
        conn.commit()
    finally:
        cursor.close()


def build_index() -> None:
    """Orchestrate the full reindexing pass from raw pages to search metadata."""
    conn = get_db()
    try:
        setup_index_tables(conn)
        pages = fetch_pages(conn)
        page_count = len(pages)
        log.info("Total pages: %s", page_count)

        if page_count == 0:
            log.warning("No pages found. Run scripts/python/crawler.py first.")
            return

        log.info("Pass 1/3: tokenizing title/body fields...")
        doc_maps, doc_lens, df_map = build_document_maps(pages)
        avg_doc_len = sum(doc_lens.values()) / page_count if page_count else 0.0

        log.info("Pass 2/3: inserting %s terms with BM25 IDF...", len(df_map))
        term_id_map = insert_terms(conn, df_map, page_count)

        log.info("Pass 3/3: writing postings and graph-based page rank...")
        written_postings = insert_postings(conn, pages, doc_maps, term_id_map)
        page_ids = [page["id"] for page in pages]
        write_page_rank(conn, page_ids, doc_lens)

        upsert_index_meta(
            conn,
            [
                ("bm25_k1", BM25_K1, None),
                ("bm25_b", BM25_B, None),
                ("title_boost", TITLE_BOOST, None),
                ("page_rank_weight", PAGE_RANK_WEIGHT, None),
                ("coverage_weight", COVERAGE_WEIGHT, None),
                ("phrase_weight", PHRASE_WEIGHT, None),
                ("proximity_weight", PROXIMITY_WEIGHT, None),
                ("avg_doc_len", avg_doc_len, None),
                ("page_count", float(page_count), None),
                ("term_count", float(len(df_map)), None),
                ("ranking_strategy", None, "bm25_field_boost_graph_pr_positional_spell"),
                ("last_index_unix", float(time.time()), None),
            ],
        )

        log.info("Indexing complete. Terms=%s Postings=%s", len(df_map), written_postings)
    finally:
        conn.close()


if __name__ == "__main__":
    build_index()
