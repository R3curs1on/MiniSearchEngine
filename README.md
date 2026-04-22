# MiniSearch (Python + MySQL + Express + HTML/CSS/JS)

Crawl -> Index -> Search with BM25 ranking, field boosting, graph-based page rank, spelling correction, and positional boosts.

## Architecture

```text
python python/crawler.py --seed <URL> --max <N>
        |
        v  MySQL (pages + crawl_log + crawl_edges)
python python/indexer.py
        |
        v  MySQL (terms + postings + page_rank + index_meta)
node backend/server.js
        |
        v  http://localhost:3001/index.html
```

## Ranking model

- BM25 scoring (`k1`, `b`) with precomputed IDF in `terms.idf`.
- Field boosting using weighted term frequency (`title_boost` > body weight).
- Graph-based page rank from `crawl_edges` link graph (iterative damping model).
- Final search score:
  - `final_score = bm25_sum + page_rank_weight * page_rank + coverage_weight * coverage + phrase_weight * phrase_score + proximity_weight * proximity_score`
- Tunables stored in `index_meta` so API/UI can show diagnostics.

## Prerequisites

- Python 3.10+
- Node.js 18+
- MySQL 8+

## Setup

1. Create database and app user:

```sql
CREATE DATABASE search_engine CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'mini_search'@'localhost' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON search_engine.* TO 'mini_search'@'localhost';
FLUSH PRIVILEGES;
```

2. Configure environment:

```bash
cp .env.example .env
```

Example `.env`:

```env
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=mini_search
MYSQL_PASSWORD=change_me
MYSQL_DATABASE=search_engine
PORT=3001
```

3. Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
npm install
```

## Run

### Option A: Step by step

```bash
python python/crawler.py --seed https://en.wikipedia.org/wiki/Main_Page --max 120 --fresh
python python/indexer.py
node backend/server.js
```

Open `http://localhost:3001/index.html`.

### Option B: Single command pipeline

```bash
npm run pipeline -- https://en.wikipedia.org/wiki/Main_Page 120
```

The pipeline now passes `--fresh` by default so switching corpora replaces the old stored pages instead of mixing sources.

This runs:

1. crawler
2. indexer
3. API/UI server

## API endpoints

- `GET /search?q=<query>&page=1&limit=10`
- `GET /suggest?q=<prefix>`
- `GET /stats`
- `GET /metrics`
- `GET /health`

### Search example

```bash
curl "http://localhost:3001/search?q=python%20decorators&page=1&limit=5"
```

Search response includes:

- ranked results
- score signal breakdown (`bm25`, `page_rank`, `coverage`, `phrase`, `proximity`)
- diagnostics (`strategy`, BM25 params, matched terms, corrections)

## Tests

```bash
npm test
```

This runs:

- JS tests (`tests/search_utils.test.js`)
- Python tests (`tests/test_indexer_tokenizer.py`, `tests/test_crawler_helpers.py`)

## Re-indexing behavior

- `python/indexer.py` rebuilds index tables (`terms`, `postings`, `page_rank`, `index_meta`) from current `pages` + `crawl_edges`.
- `pages` and crawl history remain intact, so rerunning crawl + index refreshes ranking data without losing crawl logs.

## Files of interest

- `python/crawler.py`: crawler + crawl edge capture
- `python/indexer.py`: BM25 index build + page rank computation
- `backend/server.js`: Express API, diagnostics, metrics, health checks
- `backend/search_utils.js`: shared tokenization/highlight helpers
- `public/index.html`, `public/style.css`, `public/script.js`: frontend UI
