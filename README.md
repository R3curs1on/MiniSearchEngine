# MiniSearch (Python + MySQL + Express + HTML/CSS/JS)

Crawl -> Index -> Search with BM25 ranking, field boosting, and graph-based page rank.

## Architecture

```text
python crawler.py --seed <URL> --max <N>
        |
        v  MySQL (pages + crawl_log + crawl_edges)
python indexer.py
        |
        v  MySQL (terms + postings + page_rank + index_meta)
node server.js
        |
        v  http://localhost:3001/index.html
```

## Ranking model

- BM25 scoring (`k1`, `b`) with precomputed IDF in `terms.idf`.
- Field boosting using weighted term frequency (`title_boost` > body weight).
- Graph-based page rank from `crawl_edges` link graph (iterative damping model).
- Final search score:
  - `final_score = bm25_sum + page_rank_weight * page_rank + coverage_weight * coverage`
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
python crawler.py --seed https://docs.python.org/3/ --max 120
python indexer.py
node server.js
```

Open `http://localhost:3001/index.html`.

### Option B: Single command pipeline

```bash
npm run pipeline -- https://docs.python.org/3/ 120
```

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
- score signal breakdown (`bm25`, `page_rank`, `coverage`)
- diagnostics (`strategy`, BM25 params, matched terms)

## Tests

```bash
npm test
```

This runs:

- JS tests (`tests/search_utils.test.js`)
- Python tests (`tests/test_indexer_tokenizer.py`)

## Re-indexing behavior

- `indexer.py` rebuilds index tables (`terms`, `postings`, `page_rank`, `index_meta`) from current `pages` + `crawl_edges`.
- `pages` and crawl history remain intact, so rerunning crawl + index refreshes ranking data without losing crawl logs.

## Files of interest

- `crawler.py`: crawler + crawl edge capture
- `indexer.py`: BM25 index build + page rank computation
- `server.js`: Express API, diagnostics, metrics, health checks
- `search_utils.js`: shared tokenization/highlight helpers
- `index.html`, `style.css`, `script.js`: frontend UI
