// server.js - Express Search API (MySQL version)

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");

const { tokenize, highlight } = require("./search_utils");

function loadDotEnv() {
  const envPath = path.join(__dirname, ".env");
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
  strategy: "bm25_field_boost_graph_pr",
  k1: 1.2,
  b: 0.75,
  titleBoost: 2.5,
  pageRankWeight: 0.35,
  coverageWeight: 0.2,
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

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

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

  const terms = tokenize(rawQuery);
  const ranking = await loadRankingConfig();
  const diagnostics = {
    strategy: ranking.strategy,
    params: {
      k1: ranking.k1,
      b: ranking.b,
      title_boost: ranking.titleBoost,
      page_rank_weight: ranking.pageRankWeight,
      coverage_weight: ranking.coverageWeight,
      avg_doc_len: ranking.avgDocLen,
    },
    requested_terms: terms,
    matched_terms: [],
  };

  if (!rawQuery || !terms.length) {
    return res.json({
      query: rawQuery,
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

    if (!termRows.length) {
      return res.json({
        query: rawQuery,
        total: 0,
        page,
        limit,
        took_ms: Date.now() - started,
        diagnostics,
        results: [],
      });
    }

    diagnostics.matched_terms = termRows.map((row) => row.term);

    const termIds = termRows.map((row) => row.id);
    const idPH = termIds.map(() => "?").join(",");
    const numTerms = termIds.length;

    const [countRows] = await pool.query(
      `SELECT COUNT(DISTINCT page_id) AS total FROM postings WHERE term_id IN (${idPH})`,
      termIds
    );
    const total = Number(countRows[0]?.total || 0);

    const bm25Expr = `
      t.idf * (
        ((po.tf_body + (po.tf_title * ${ranking.titleBoost})) * (${ranking.k1} + 1))
        /
        (
          (po.tf_body + (po.tf_title * ${ranking.titleBoost}))
          + ${ranking.k1}
            * (1 - ${ranking.b} + ${ranking.b} * (COALESCE(pr.word_count, 0) / ${ranking.avgDocLen}))
        )
      )
    `;

    const [rows] = await pool.query(
      `
      SELECT
        ranked.id,
        ranked.url,
        ranked.title,
        ranked.body_text,
        ranked.bm25_sum,
        ranked.term_hits,
        ranked.coverage,
        ranked.page_rank,
        (
          ranked.bm25_sum
          + (${ranking.pageRankWeight} * ranked.page_rank)
          + (${ranking.coverageWeight} * ranked.coverage)
        ) AS final_score
      FROM (
        SELECT
          p.id,
          p.url,
          p.title,
          p.body_text,
          SUM(${bm25Expr}) AS bm25_sum,
          COUNT(DISTINCT po.term_id) AS term_hits,
          CAST(COUNT(DISTINCT po.term_id) AS DECIMAL(10,4)) / ${numTerms} AS coverage,
          COALESCE(pr.score, 0.0) AS page_rank
        FROM postings po
        JOIN terms t ON t.id = po.term_id
        JOIN pages p ON p.id = po.page_id
        LEFT JOIN page_rank pr ON pr.page_id = po.page_id
        WHERE po.term_id IN (${idPH})
        GROUP BY p.id
      ) AS ranked
      ORDER BY final_score DESC
      LIMIT ? OFFSET ?
      `,
      [...termIds, limit, offset]
    );

    const results = rows.map((row) => ({
      id: row.id,
      url: String(row.url || ""),
      title: String(row.title || row.url || ""),
      snippet: highlight(row.body_text, terms),
      score: parseFloat(Number(row.final_score || 0).toFixed(4)),
      term_hits: Number(row.term_hits || 0),
      coverage: parseFloat(Number(row.coverage || 0).toFixed(3)),
      signals: {
        bm25: parseFloat(Number(row.bm25_sum || 0).toFixed(4)),
        page_rank: parseFloat(Number(row.page_rank || 0).toFixed(4)),
        coverage: parseFloat(Number(row.coverage || 0).toFixed(3)),
      },
    }));

    return res.json({
      query: rawQuery,
      total,
      page,
      limit,
      took_ms: Date.now() - started,
      diagnostics,
      results,
    });
  } catch (error) {
    console.error("Search error:", error);
    return res.status(500).json({
      error: "Search failed",
      diagnostics,
    });
  }
});

app.get("/suggest", async (req, res) => {
  const q = String(req.query.q || "").trim().toLowerCase();
  if (q.length < 2) return res.json([]);
  try {
    const [rows] = await pool.query(
      "SELECT term FROM terms WHERE term LIKE ? AND doc_freq > 1 ORDER BY doc_freq DESC LIMIT 8",
      [`${q}%`]
    );
    return res.json(rows.map((row) => row.term));
  } catch {
    return res.json([]);
  }
});

app.get("/stats", async (_req, res) => {
  try {
    const [rows] = await pool.query(
      `
      SELECT
        (SELECT COUNT(*) FROM pages) AS pages,
        (SELECT COUNT(*) FROM terms) AS terms,
        (SELECT COUNT(*) FROM postings) AS postings
      `
    );
    const ranking = await loadRankingConfig();
    return res.json({
      ...rows[0],
      ranking_strategy: ranking.strategy,
      avg_doc_len: ranking.avgDocLen,
    });
  } catch {
    return res.status(500).json({ error: "Stats unavailable" });
  }
});

app.get("/metrics", async (_req, res) => {
  try {
    const [rows] = await pool.query(
      `
      SELECT
        (SELECT COUNT(*) FROM pages) AS pages,
        (SELECT COUNT(*) FROM terms) AS terms,
        (SELECT COUNT(*) FROM postings) AS postings,
        (SELECT COUNT(*) FROM crawl_log) AS crawl_attempts,
        (SELECT COUNT(*) FROM crawl_log WHERE status = 200) AS crawl_success,
        (SELECT ROUND(AVG(word_count), 2) FROM page_rank) AS avg_doc_len,
        (SELECT ROUND(AVG(score), 6) FROM page_rank) AS avg_page_rank,
        (SELECT MAX(crawled_at) FROM pages) AS last_crawled_at
      `
    );
    const ranking = await loadRankingConfig();
    return res.json({
      ...rows[0],
      ranking,
      uptime_s: Number(((Date.now() - bootTime) / 1000).toFixed(1)),
    });
  } catch (error) {
    return res.status(500).json({ error: "Metrics unavailable", message: error.message });
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
      console.log(`Connected to MySQL (attempt ${attempt}/${retries})`);
      return;
    } catch (error) {
      lastError = error;
      console.error(`MySQL connection failed (attempt ${attempt}/${retries}): ${error.message}`);
      if (attempt < retries) await sleep(delayMs);
    }
  }
  throw lastError || new Error("Could not connect to MySQL");
}

async function start() {
  try {
    await connectWithRetry();
    await loadRankingConfig(true);
  } catch (error) {
    console.error("ERROR: could not connect to MySQL");
    console.error(error.message);
    process.exit(1);
  }

  app.listen(PORT, () => {
    console.log(`\nSearch API  -> http://localhost:${PORT}`);
    console.log(`Search UI   -> http://localhost:${PORT}/index.html`);
    console.log(`Health      -> http://localhost:${PORT}/health`);
    console.log(`Metrics     -> http://localhost:${PORT}/metrics\n`);
  });
}

start();
