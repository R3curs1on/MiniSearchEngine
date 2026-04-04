// server.js - Express Search API (MySQL version)

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");

const {
  tokenize,
  highlight,
  buildLexicon,
  correctTerms,
  computePositionalSignals,
} = require("./search_utils");

const ROOT_DIR = path.join(__dirname, "..");
const PUBLIC_DIR = path.join(ROOT_DIR, "public");

function loadDotEnv() {
  const envPath = path.join(ROOT_DIR, ".env");
  if (!fs.existsSync(envPath)) return;

  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;

    const eqIndex = line.indexOf("=");
    if (eqIndex === -1) continue;

    const key = line.slice(0, eqIndex).trim();
    const value = line.slice(eqIndex + 1).trim().replace(/^['"]|['"]$/g, "");
    if (!(key in process.env)) process.env[key] = value;
  }
}

loadDotEnv();

function requireEnv(name) {
  const value = process.env[name];
  if (!value) {
    throw new Error(
      `Missing required MySQL configuration: ${name}. ` +
      "Copy .env.example to .env and set real credentials for a dedicated MySQL user."
    );
  }
  return value;
}

function toSafeNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

const DEFAULT_RANKING_CONFIG = {
  strategy: "bm25_field_boost_graph_pr_positional_spell",
  k1: 1.2,
  b: 0.75,
  titleBoost: 2.5,
  pageRankWeight: 0.35,
  coverageWeight: 0.2,
  phraseWeight: 0.9,
  proximityWeight: 0.45,
  avgDocLen: 200,
};

const app = express();
const PORT = Number(process.env.PORT || 3001);
const bootTime = Date.now();

const pool = mysql.createPool({
  host: process.env.MYSQL_HOST || "127.0.0.1",
  port: Number(process.env.MYSQL_PORT || 3306),
  user: requireEnv("MYSQL_USER"),
  password: requireEnv("MYSQL_PASSWORD"),
  database: requireEnv("MYSQL_DATABASE"),
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

let rankingConfigCache = {
  expiresAt: 0,
  value: { ...DEFAULT_RANKING_CONFIG },
};

let lexiconCache = {
  expiresAt: 0,
  value: buildLexicon([]),
};

app.use(cors());
app.use(express.json());
app.use(express.static(PUBLIC_DIR));

async function loadRankingConfig(force = false) {
  const now = Date.now();
  if (!force && rankingConfigCache.expiresAt > now) return rankingConfigCache.value;

  try {
    const [rows] = await pool.query(
      `
      SELECT key_name, value_num, value_text
      FROM index_meta
      WHERE key_name IN (
        'ranking_strategy',
        'bm25_k1',
        'bm25_b',
        'title_boost',
        'page_rank_weight',
        'coverage_weight',
        'phrase_weight',
        'proximity_weight',
        'avg_doc_len'
      )
      `
    );

    const map = new Map(rows.map((row) => [row.key_name, row]));
    const config = {
      strategy: map.get("ranking_strategy")?.value_text || DEFAULT_RANKING_CONFIG.strategy,
      k1: toSafeNumber(map.get("bm25_k1")?.value_num, DEFAULT_RANKING_CONFIG.k1),
      b: toSafeNumber(map.get("bm25_b")?.value_num, DEFAULT_RANKING_CONFIG.b),
      titleBoost: toSafeNumber(map.get("title_boost")?.value_num, DEFAULT_RANKING_CONFIG.titleBoost),
      pageRankWeight: toSafeNumber(
        map.get("page_rank_weight")?.value_num,
        DEFAULT_RANKING_CONFIG.pageRankWeight
      ),
      coverageWeight: toSafeNumber(
        map.get("coverage_weight")?.value_num,
        DEFAULT_RANKING_CONFIG.coverageWeight
      ),
      phraseWeight: toSafeNumber(
        map.get("phrase_weight")?.value_num,
        DEFAULT_RANKING_CONFIG.phraseWeight
      ),
      proximityWeight: toSafeNumber(
        map.get("proximity_weight")?.value_num,
        DEFAULT_RANKING_CONFIG.proximityWeight
      ),
      avgDocLen: Math.max(1, toSafeNumber(map.get("avg_doc_len")?.value_num, DEFAULT_RANKING_CONFIG.avgDocLen)),
    };

    rankingConfigCache = {
      value: config,
      expiresAt: now + 30_000,
    };
    return config;
  } catch {
    rankingConfigCache = {
      value: { ...DEFAULT_RANKING_CONFIG },
      expiresAt: now + 10_000,
    };
    return rankingConfigCache.value;
  }
}

async function loadTermLexicon(force = false) {
  const now = Date.now();
  if (!force && lexiconCache.expiresAt > now) return lexiconCache.value;

  try {
    const [rows] = await pool.query(
      `
      SELECT term, doc_freq
      FROM terms
      ORDER BY doc_freq DESC, term ASC
      `
    );

    lexiconCache = {
      value: buildLexicon(rows),
      expiresAt: now + 60_000,
    };
  } catch {
    lexiconCache = {
      value: buildLexicon([]),
      expiresAt: now + 10_000,
    };
  }

  return lexiconCache.value;
}

function parsePositions(value) {
  if (!value) return { title: [], body: [] };

  try {
    const parsed = typeof value === "string" ? JSON.parse(value) : value;
    return {
      title: Array.isArray(parsed?.title) ? parsed.title.map(Number).filter(Number.isFinite) : [],
      body: Array.isArray(parsed?.body) ? parsed.body.map(Number).filter(Number.isFinite) : [],
    };
  } catch {
    return { title: [], body: [] };
  }
}

app.get("/health", async (_req, res) => {
  const started = Date.now();
  try {
    await pool.query("SELECT 1");
    return res.json({
      status: "ok",
      db: "up",
      uptime_s: Number(((Date.now() - bootTime) / 1000).toFixed(1)),
      checked_at: new Date().toISOString(),
      latency_ms: Date.now() - started,
    });
  } catch (error) {
    return res.status(503).json({
      status: "degraded",
      db: "down",
      message: error.message,
      checked_at: new Date().toISOString(),
    });
  }
});

app.get("/search", async (req, res) => {
  const rawQuery = String(req.query.q || "").trim();
  const page = Math.max(1, parseInt(req.query.page, 10) || 1);
  const limit = Math.min(20, Math.max(1, parseInt(req.query.limit, 10) || 10));
  const offset = (page - 1) * limit;
  const started = Date.now();

  const requestedTerms = tokenize(rawQuery);
  const ranking = await loadRankingConfig();
  const lexicon = await loadTermLexicon();
  const correction = correctTerms(requestedTerms, lexicon);
  const terms = correction.terms;
  const diagnostics = {
    strategy: ranking.strategy,
    params: {
      k1: ranking.k1,
      b: ranking.b,
      title_boost: ranking.titleBoost,
      page_rank_weight: ranking.pageRankWeight,
      coverage_weight: ranking.coverageWeight,
      phrase_weight: ranking.phraseWeight,
      proximity_weight: ranking.proximityWeight,
      avg_doc_len: ranking.avgDocLen,
    },
    requested_terms: requestedTerms,
    search_terms: terms,
    correction_applied: correction.applied,
    corrected_query: correction.applied ? correction.correctedQuery : null,
    corrections: correction.corrections,
    matched_terms: [],
    unmatched_terms: [],
    candidate_pool: 0,
  };

  if (!rawQuery || !requestedTerms.length) {
    return res.json({
      query: rawQuery,
      search_query: rawQuery,
      total: 0,
      page,
      limit,
      took_ms: Date.now() - started,
      diagnostics,
      results: [],
    });
  }

  try {
    const placeholders = terms.map(() => "?").join(",");
    const [termRows] = await pool.query(
      `SELECT id, term, idf FROM terms WHERE term IN (${placeholders})`,
      terms
    );

    const termMap = new Map(termRows.map((row) => [row.term, row]));
    diagnostics.matched_terms = terms.filter((term) => termMap.has(term));
    diagnostics.unmatched_terms = terms.filter((term) => !termMap.has(term));

    if (!termRows.length) {
      return res.json({
        query: rawQuery,
        search_query: correction.correctedQuery || rawQuery,
        total: 0,
        page,
        limit,
        took_ms: Date.now() - started,
        diagnostics,
        results: [],
      });
    }

    const termIds = termRows.map((row) => row.id);
    const [candidateRows] = await pool.query(
      `
      SELECT p.page_id, p.term_id, p.tf_title, p.tf_body, p.freq_title, p.freq_body, p.positions
      FROM postings p
      WHERE p.term_id IN (${termIds.map(() => "?").join(",")})
      `,
      termIds
    );

    if (!candidateRows.length) {
      return res.json({
        query: rawQuery,
        search_query: correction.correctedQuery || rawQuery,
        total: 0,
        page,
        limit,
        took_ms: Date.now() - started,
        diagnostics,
        results: [],
      });
    }

    diagnostics.candidate_pool = candidateRows.length;

    const candidateByPage = new Map();
    for (const row of candidateRows) {
      if (!candidateByPage.has(row.page_id)) candidateByPage.set(row.page_id, []);
      candidateByPage.get(row.page_id).push(row);
    }

    const candidateIds = [...candidateByPage.keys()];
    const pagePlaceholders = candidateIds.map(() => "?").join(",");

    const [pageRows] = await pool.query(
      `
      SELECT p.id, p.url, p.title, p.body_text,
             pr.score AS page_rank, pr.word_count, pr.in_links, pr.out_links
      FROM pages p
      LEFT JOIN page_rank pr ON pr.page_id = p.id
      WHERE p.id IN (${pagePlaceholders})
      `,
      candidateIds
    );

    const pageMap = new Map(pageRows.map((row) => [row.id, row]));

    const results = [];
    for (const [pageId, rows] of candidateByPage.entries()) {
      const page = pageMap.get(pageId);
      if (!page) continue;

      let bm25Sum = 0;
      let coverageHits = 0;
      const positionsByTerm = {};

      rows.forEach((row) => {
        const termRow = termRows.find((t) => t.id === row.term_id);
        if (!termRow) return;

        const freqTitle = Number(row.freq_title || 0);
        const freqBody = Number(row.freq_body || 0);
        if (freqTitle + freqBody > 0) coverageHits += 1;

        const tfTitle = row.tf_title || 0;
        const tfBody = row.tf_body || 0;
        const idf = termRow.idf || 0;

        const docLen = Number(page.word_count || 0) || ranking.avgDocLen;
        const fieldBoost = (tfTitle * ranking.titleBoost) + tfBody;
        const denom = fieldBoost + ranking.k1 * (1 - ranking.b + ranking.b * (docLen / ranking.avgDocLen));
        const bm25 = denom > 0 ? idf * ((fieldBoost * (ranking.k1 + 1)) / denom) : 0;
        bm25Sum += bm25;

        positionsByTerm[termRow.term] = parsePositions(row.positions);
      });

      const coverage = coverageHits / Math.max(terms.length, 1);
      const positional = computePositionalSignals(terms, positionsByTerm);

      const pageRankScore = Number(page.page_rank || 0);
      const score = bm25Sum
        + (ranking.pageRankWeight * pageRankScore)
        + (ranking.coverageWeight * coverage)
        + (ranking.phraseWeight * positional.phraseScore)
        + (ranking.proximityWeight * positional.proximityScore);

      results.push({
        id: page.id,
        url: page.url,
        title: page.title,
        snippet: highlight(page.body_text || "", terms),
        score: Number(score.toFixed(4)),
        term_hits: coverageHits,
        coverage: Number(coverage.toFixed(4)),
        signals: {
          bm25: Number(bm25Sum.toFixed(4)),
          page_rank: Number(pageRankScore.toFixed(4)),
          coverage: Number(coverage.toFixed(4)),
          phrase: Number(positional.phraseScore.toFixed(4)),
          proximity: Number(positional.proximityScore.toFixed(4)),
        },
      });
    }

    results.sort((a, b) => b.score - a.score);
    const total = results.length;
    const sliced = results.slice(offset, offset + limit);

    return res.json({
      query: rawQuery,
      search_query: correction.correctedQuery || rawQuery,
      total,
      page,
      limit,
      took_ms: Date.now() - started,
      diagnostics,
      results: sliced,
    });
  } catch (error) {
    return res.status(500).json({
      error: "search_failed",
      message: error.message,
    });
  }
});

app.get("/suggest", async (req, res) => {
  const q = String(req.query.q || "").trim().toLowerCase().replace(/[^a-z]/g, "");
  if (q.length < 2) return res.json([]);
  try {
    const lexicon = await loadTermLexicon();
    return res.json(lexicon.trie.suggest(q, 8));
  } catch {
    return res.json([]);
  }
});

app.get("/stats", async (_req, res) => {
  try {
    const [[pages]] = await pool.query("SELECT COUNT(*) AS count FROM pages");
    const [[terms]] = await pool.query("SELECT COUNT(*) AS count FROM terms");
    const [[postings]] = await pool.query("SELECT COUNT(*) AS count FROM postings");

    const ranking = await loadRankingConfig();

    return res.json({
      pages: pages.count,
      terms: terms.count,
      postings: postings.count,
      ranking_strategy: ranking.strategy,
      avg_doc_len: ranking.avgDocLen,
    });
  } catch {
    return res.json({
      pages: 0,
      terms: 0,
      postings: 0,
    });
  }
});

app.get("/metrics", async (_req, res) => {
  try {
    const [[crawlAttempts]] = await pool.query("SELECT COUNT(*) AS count FROM crawl_log");
    const [[crawlSuccess]] = await pool.query("SELECT COUNT(*) AS count FROM crawl_log WHERE status = 200");
    const [[lastCrawl]] = await pool.query("SELECT MAX(crawled_at) AS last_crawled_at FROM crawl_log");
    const [[avgPageRank]] = await pool.query("SELECT AVG(score) AS avg_score FROM page_rank");

    return res.json({
      uptime_s: Number(((Date.now() - bootTime) / 1000).toFixed(1)),
      crawl_attempts: crawlAttempts.count,
      crawl_success: crawlSuccess.count,
      last_crawled_at: lastCrawl.last_crawled_at,
      avg_page_rank: Number(avgPageRank.avg_score || 0),
    });
  } catch (error) {
    return res.status(500).json({
      error: "metrics_failed",
      message: error.message,
    });
  }
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function connectWithRetry(retries = 6, delayMs = 1500) {
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    try {
      await pool.query("SELECT 1");
      return;
    } catch (err) {
      lastError = err;
      await sleep(delayMs);
    }
  }
  throw lastError || new Error("Could not connect to MySQL");
}

async function start() {
  try {
    await connectWithRetry();
  } catch (error) {
    console.error("Failed to connect to MySQL:", error.message);
    process.exit(1);
  }

  app.listen(PORT, () => {
    console.log(`API + UI listening on http://localhost:${PORT}`);
  });
}

start();
