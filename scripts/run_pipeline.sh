#!/usr/bin/env bash
set -euo pipefail

SEED_URL="${1:-https://en.wikipedia.org/wiki/Main_Page}"
MAX_PAGES="${2:-100}"
FRESH_FLAG="${FRESH_FLAG:---fresh}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -f ".env" ]]; then
  echo "Missing .env. Copy .env.example to .env and set MySQL credentials."
  exit 1
fi

if ! command -v "${PYTHON_BIN}" >/dev/null 2>&1; then
  echo "Missing Python interpreter: ${PYTHON_BIN}"
  exit 1
fi

echo "==> Crawling seed: ${SEED_URL} (max=${MAX_PAGES})"
"${PYTHON_BIN}" scripts/python/crawler.py --seed "${SEED_URL}" --max "${MAX_PAGES}" ${FRESH_FLAG}

echo "==> Building BM25 index"
"${PYTHON_BIN}" scripts/python/indexer.py

echo "==> Starting API and UI on http://localhost:3001"
node backend/server.js
