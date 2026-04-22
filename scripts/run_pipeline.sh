#!/usr/bin/env bash
set -euo pipefail

SEED_URL="${1:-https://en.wikipedia.org/wiki/Main_Page}"
MAX_PAGES="${2:-100}"
FRESH_FLAG="${FRESH_FLAG:---fresh}"

if [[ ! -f ".env" ]]; then
  echo "Missing .env. Copy .env.example to .env and set MySQL credentials."
  exit 1
fi

echo "==> Crawling seed: ${SEED_URL} (max=${MAX_PAGES})"
python python/crawler.py --seed "${SEED_URL}" --max "${MAX_PAGES}" ${FRESH_FLAG}

echo "==> Building BM25 index"
python python/indexer.py

echo "==> Starting API and UI on http://localhost:3001"
node backend/server.js
