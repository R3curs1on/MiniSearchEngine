"""
indexer.py — TF-IDF Inverted Index Builder (MySQL version)
Reads crawled pages from MySQL and builds the inverted index in-place.

Run: python indexer.py
"""

import math
import json
# from operator import pos
import re
import logging
from collections import defaultdict

from db import get_db as get_mysql_db

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

stopwords = """
    a about above after again against all am an and any are as at be because been
    before being below between both but by did do does doing down during each few
    for from get got have he her here him his how i if in into is it its let me
    more most my no nor not of off on once only or other our out over own same she
    should so some such than that the their them then there these they this those
    through to too under until up very was we were what when where which while who
    whom why will with you your
    """
STOPWORDS = set(stopwords.split())

# ─── DB connection ────────────────────────────────────────────────────────────

def get_db():
    return get_mysql_db()

def setup_index_tables(conn):
    cursor = conn.cursor()
    '''
    We create three tables:
        1. `terms`: this table stores unique terms extracted from the pages (term varchar(255)), along with their document frequency (df ie the number of pages that contain the term) [doc_freq (INT)].
                    it has an auto-incrementing primary key `id` [id BIGINT PRIMARY KEY AUTO_INCREMENT]
                    and a unique index on the `term` column to ensure that each term is stored only once [UNIQUE KEY uq_terms_term (term)], 
                    along with an additional index on the `term` column to speed up lookups when we need to find the term_id for a given term during indexing and searching [KEY idx_terms_term (term)].

        2. `postings`:  this table is used for actual score computation and storing the inverted index; we use this table to search which page is important according to query
                        this table stores the postings list for each term (term_id BIGINT) (which is foreign key to `terms`), 
                        page_id (BIGINT) (foreign key to `pages`),
                        tf_idf score (DOUBLE),
                        term frequency (tf DOUBLE),
                        and positions of the term in the page (positions (JSON))
                        the primary key is a composite of (term_id, page_id) to ensure that each term-page pair is unique
                        we also create indexes to speed up lookups by term_id (KEY idx_postings_term (term_id)) 
                        sorting by tf_idf score (KEY idx_postings_score (term_id, tf_idf)).


        3. `page_rank`: this table is mainly used to show page order based on basic criteria ( longer page assumed as more information )
                        this table stores a simple page rank score for each page, which can be used to boost search results based on the length of the page (as a proxy for importance).
                        it has a primary key on page_id (BIGINT PRIMARY KEY),
                        a score column (DOUBLE DEFAULT 1.0) which we compute as log(word_count + 1) during indexing,
                        and word_count column (INT DEFAULT 0) to store the number of words in the page.
    '''
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
    # we use a simple regex to extract words of length 2 or more, convert them to lowercase, and filter out stopwords;
    # this gives us a only list of meaningful tokens that we can use for indexing and searching
    # for optimization we can use a dict to store <term , list<positions>> while we iterate through tokens once and limit the list of positions to 50 for very common terms to avoid storing too much data;
    
    words = re.findall(r"[a-z]{2,}", text.lower())
    token_map = {}

    for i, term in enumerate(words):
        if term in STOPWORDS:
            continue

        if term not in token_map:
            token_map[term] = []

        if len(token_map[term]) < 50:
            token_map[term].append(i)

    return token_map


def compute_tf(token_map):
     # term freq = (number of times term appears in document) / (total number of terms in document); 
    # we count the frequency of each term in the list of tokens and then divide by the total number of tokens to get the term frequency; 
    # this gives us a normalized measure of how important a term is within a specific document, which we can use in conjunction with IDF to compute TF-IDF scores for ranking search results
    total = sum(len(pos_list) for pos_list in token_map.values()) or 1  # total cant be zero cuz cant divide by zero
    return {
        term: len(pos_list) / total
        for term, pos_list in token_map.items()
    }

def get_positions(tokens, term):
    # we find all the positions (indices) of the given term in the list of tokens and return them as a list; this allows us to store the positions of each term within the document, which can be useful for phrase searching and proximity queries in our search engine
    # we run a for loop in tokens ; it currToken == term then we add the index to the list of positions; we limit to first 50 positions to avoid storing too much data for very common terms
    
    # return [i for i, t in enumerate(tokens) if t == term][:50]

    # this can be optimized by using a more efficient data structure or algorithm if needed, but for simplicity we just do a linear scan here; in practice, we might want to consider more efficient ways to store and retrieve term positions, especially for large documents with many occurrences of the same term
    # for optimization we can use a dict to store <term , list<positions>> while we iterate through tokens once and limit the list of positions to 50 for very common terms to avoid storing too much data;
    return tokens.get(term, [])[:50]

# ─── Indexer ──────────────────────────────────────────────────────────────────

def build_index():
    conn = get_db()
    setup_index_tables(conn)
    cursor = conn.cursor(dictionary=True)

    # Clear old index (re-index from scratch)

    #but for development we can comment out the below lines to avoid deleting old index and just update it with new data; this allows us to incrementally build our index as we crawl more pages without losing the existing indexed data, which can be useful for testing and development purposes; however, in a production environment, we might want to clear the old index before rebuilding to ensure that we have a clean slate and avoid potential issues with stale data
    # cursor.execute("DELETE FROM postings")
    # cursor.execute("DELETE FROM terms")
    # cursor.execute("DELETE FROM page_rank")
    conn.commit()

    log.info("Loading pages...")
    # we load all the pages from the `pages` table in our MySQL database, which contains the crawled pages with their id, title, and body text; 
    # we store the total number of pages (N) for later use in computing IDF scores; if no pages are found, we log a warning and exit, suggesting to run crawler.py first to populate the database with pages before building the index
    cursor.execute("SELECT id, title, body_text FROM pages")
    pages = cursor.fetchall()
    N     = len(pages)
    log.info(f"Total pages: {N}")

    if N == 0:
        log.warning("No pages found. Run crawler.py first.")
        conn.close(); return

    # ── Pass 1: tokenize + build document-frequency map ──────────────────────
    '''
    In the first pass, we iterate through all the pages and tokenize their content (title + body text) using the `tokenize` function, which extracts meaningful terms while filtering out stopwords.
    We store the tokens for each page in a dictionary (`doc_tokens`) for later use in the second pass, where we will compute TF-IDF scores and write postings to the database.
    At the same time, we build a document-frequency map (`df_map`) that keeps track of which pages contain each term, allowing us to compute the document frequency (df) for each term, which is essential for calculating the IDF component of the TF-IDF score in the second pass. 
    This two-pass approach allows us to efficiently compute the necessary statistics for all terms before we start writing the postings, ensuring that we have all the information we need to compute accurate TF-IDF scores when we insert the postings into the database.
    '''
    log.info("Pass 1: Tokenizing all pages...")
    doc_tokens = {}          # page_id → [tokens]
    df_map     = defaultdict(set)   # term → {page_ids}

    for page in pages:
        pid    = page["id"]
        text   = (page["title"] or "") + " " + (page["body_text"] or "")
        tokens = tokenize(text)   # only useful words ; no stopwords and we also store the positions of each term in the page for later use in phrase searching and proximity queries; this gives us a mapping of terms to their positions within the document, which can be useful for advanced search features in our search engine
        doc_tokens[pid] = tokens # store tokens for later use in Pass 2 ; cuz tokens is dict <term,list<pos>>
        for term in tokens.keys():
            df_map[term].add(pid)

    # ── Insert terms ──────────────────────────────────────────────────────────
    log.info(f"Inserting {len(df_map)} unique terms...")
    insert_terms = conn.cursor()
    '''
    we insert the unique terms into the `terms` table along with their document frequency (the number of pages that contain the term) using an `executemany` statement for efficient batch insertion;
    executemany allows us to insert multiple rows in a single query, which is much faster than inserting one row at a time, especially when we have a large number of terms to insert;
    we use the ON DUPLICATE KEY UPDATE clause to handle cases where a term already exists in the `terms` table (which can happen if we run the indexer multiple times without clearing  the old index); 
    if a term already exists, we update its document frequency to the new value (which is the number of pages that contain the term based on our current tokenization);
    this ensures that our index remains accurate and up-to-date even if we run the indexer multiple times as we crawl more pages and discover new terms or update existing terms' document frequencies.
    '''
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
        pid  = page["id"]  #  curr page id
        curr_tokens = doc_tokens[pid] # tokens for current page from pass 1 ; this is a dict <term, list<pos>> which gives us the terms in the page and their positions; 
        if not curr_tokens:
            continue

        tf_map     = compute_tf(curr_tokens) # term freq map for current page ; this is dict <term,tf>
        word_count = sum( len(pos_list) for pos_list in curr_tokens.values())  # total number of words in the page
        
        if word_count == 0:
            log.warning(f"Empty doc: {pid}")    

        pr_score   = math.log(word_count + 1)    
        # pr score is page rank score ; rn we are assuming that longer pages are more important and we use log(word_count + 1) as a simple page rank score;
        # this is a very basic heuristic for page importance, and in a real search engine, we would likely want to use a more sophisticated algorithm for computing page rank based on the link structure of the web;
        # however, for our mini search engine, this simple approach can help boost the ranking of longer pages in search results, which can be beneficial since longer pages often contain more content and may be more likely to satisfy user queries.

        page_rank_batch.append((pid, pr_score, word_count))

        for term, tf in tf_map.items():
            '''
            last stage of second pass ;
            for each page we calculate : df, idf and TF-IDF score
            df from df_map
            idf from N and df ( log (N/df) )
            tf from tf_map
            tf-idf = tf * idf
                we also get the term_id for the term from term_id_map to store in the postings table;
                we get the positions of the term in the page from curr_tokens (which is dict <term, list<pos>>) and convert it to JSON format to store in the postings table;
            '''
            tid = term_id_map.get(term)
            if not tid: continue
            df    = len(df_map[term])
            idf   = math.log(N / df) if df > 0 else 0
            score = tf * idf
            # pos   = json.dumps(get_positions(tokens, term))
            # pos = json.dumps(get_positions(curr_tokens, term))
            pos = json.dumps(curr_tokens[term])
            postings_batch.append((tid, pid, score, tf, pos))
            written += 1

            if len(postings_batch) >= BATCH_SIZE:
                postings_cursor = conn.cursor()
                '''
                we batch the postings and insert them into the `postings` table using an executemany statement for efficiency, 
                and we use the ON DUPLICATE KEY UPDATE clause to handle cases where a term-page pair already exists in the `postings` table (which can happen if we run the indexer multiple times without clearing the old index); 
                if a term-page pair already exists, we update the tf_idf score, term frequency (tf), and positions to the new values based on our current computation;
                '''
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
    '''
    Flush remaining postings that didn’t fill a full batch.
    This ensures no data is lost when total postings < BATCH_SIZE multiple.
    '''
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
