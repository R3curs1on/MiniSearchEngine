# MiniSearch — MySQL Edition

> Crawl -> Index -> Search. Data lives in MySQL instead of `search_engine.db`.

## How it works

```text
python crawler.py --seed <URL>
        |
        v  MySQL (pages table)
python indexer.py
        |
        v  MySQL (terms + postings + page_rank tables)
node server.js
        |
        v  http://localhost:3001
   index.html
```

## Setup

1. Create a MySQL database and app user:

```sql
CREATE DATABASE search_engine CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'mini_search'@'localhost' IDENTIFIED BY 'change_me';
GRANT ALL PRIVILEGES ON search_engine.* TO 'mini_search'@'localhost';
FLUSH PRIVILEGES;
```

2. Configure environment variables:

```bash
cp .env.example .env
```

The Python scripts and `server.js` will auto-load values from `.env`. Exporting
the same `MYSQL_*` variables in your shell still works if you prefer that.

Do not use the MySQL `root` account for the app on Ubuntu/Mint unless you have
explicitly configured password auth for it. The default `root` setup commonly
uses socket auth and will fail from Python/Node clients with `Access denied`.

3. Install dependencies:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

npm install
```

4. Run the pipeline:

```bash
python crawler.py --seed https://docs.python.org/3/ --max 100
python indexer.py
node server.js
```

Then open `http://localhost:3001/index.html`.

## Notes

- `crawler.py` creates the `pages` and `crawl_log` tables if they do not exist.
- The `pages` table stores a SHA-256 `url_hash` for URL uniqueness because MySQL cannot safely index a full `VARCHAR(2000)` URL under `utf8mb4`.
- `indexer.py` creates the `terms`, `postings`, and `page_rank` tables if they do not exist.
- `server.js` reads MySQL connection details from the same `MYSQL_*` environment variables.
