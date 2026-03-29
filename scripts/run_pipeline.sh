#!/usr/bin/env bash
set -euo pipefail

SEED_URL="${1:-https://docs.python.org/3/}"
MAX_PAGES="${2:-100}"

if [[ ! -f ".env" ]]; then
  echo "Missing .env. Copy .env.example to .env and set MySQL credentials."
  exit 1
fi

echo "==> Crawling seed: ${SEED_URL} (max=${MAX_PAGES})"
python crawler.py --seed "${SEED_URL}" --max "${MAX_PAGES}"

echo "==> Building BM25 index"
python indexer.py

echo "==> Starting API and UI on http://localhost:3001"
node server.js
