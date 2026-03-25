// server.js — Express Search API (MySQL version)

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");

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
    if (!(key in process.env)) {
      process.env[key] = value;
    }
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

const app = express();
const PORT = process.env.PORT || 3001;
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

app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));   // serves index.html from same folder

// ─── Helpers ──────────────────────────────────────────────────────────────────

const STOPWORDS = new Set([
  "a","about","above","after","again","all","am","an","and","any","are","as",
  "at","be","because","been","before","being","below","between","both","but",
  "by","did","do","does","doing","down","each","few","for","from","get","got",
  "have","he","her","here","him","his","how","i","if","in","into","is","it",
  "its","let","me","more","most","my","no","nor","not","of","off","on","once",
  "only","or","other","our","out","over","own","same","she","should","so",
  "some","such","than","that","the","their","them","then","there","these",
  "they","this","those","through","to","too","under","until","up","very","was",
  "we","were","what","when","where","which","while","who","whom","why","will",
  "with","you","your"
]);

function tokenize(query) {
  return query
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .split(/\s+/)
    .filter(t => t.length >= 2 && !STOPWORDS.has(t));
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, char => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  })[char]);
}

function highlight(text, terms, snippetLen = 220) {
  if (!text) return "";
  const escapedTerms = terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  if (!escapedTerms.length) {
    const baseSnippet = text.slice(0, snippetLen) + (text.length > snippetLen ? "…" : "");
    return escapeHtml(baseSnippet);
  }

  const regex = new RegExp(`(${escapedTerms.join("|")})`, "gi");
  const match = regex.exec(text);
  if (!match) {
    const baseSnippet = text.slice(0, snippetLen) + (text.length > snippetLen ? "…" : "");
    return escapeHtml(baseSnippet);
  }

  const start = Math.max(0, match.index - 80);
  const end = Math.min(text.length, start + snippetLen);
  const snippet = (start > 0 ? "…" : "") + text.slice(start, end) + (end < text.length ? "…" : "");
  regex.lastIndex = 0;

  let html = "";
  let lastIndex = 0;
  for (const snippetMatch of snippet.matchAll(regex)) {
    const index = snippetMatch.index ?? 0;
    html += escapeHtml(snippet.slice(lastIndex, index));
    html += `<mark>${escapeHtml(snippetMatch[0])}</mark>`;
    lastIndex = index + snippetMatch[0].length;
  }

  html += escapeHtml(snippet.slice(lastIndex));
  return html;
}

app.get("/search", async (req, res) => {
    const rawQuery = (req.query.q || "").trim();
    const page = Math.max(1,  parseInt(req.query.page, 10) || 1);
    const limit = Math.min(20, parseInt(req.query.limit, 10) || 10);
    const offset = (page - 1) * limit;

    if (!rawQuery) return res.json({ results: [], total: 0, query: rawQuery });

    const terms = tokenize(rawQuery);
    if (!terms.length) return res.json({ results: [], total: 0, query: rawQuery });

    const start = Date.now();

    try {
      // Get term IDs
      const placeholders = terms.map(() => "?").join(",");
      const [termRows] = await pool.query(
        `SELECT id, term FROM terms WHERE term IN (${placeholders})`,
        terms
      );
      if (!termRows.length)
        return res.json({ results: [], total: 0, query: rawQuery, took_ms: Date.now() - start });

      const termIds  = termRows.map(r => r.id);
      const idPH     = termIds.map(() => "?").join(",");
      const numTerms = termIds.length;

      // Total matched pages
      const [countRows] = await pool.query(
        `SELECT COUNT(DISTINCT page_id) AS total FROM postings WHERE term_id IN (${idPH})`,
        termIds
      );
      const total = countRows[0] ? countRows[0].total : 0;

      // Ranked results
      const [rows] = await pool.query(`
        SELECT
          p.id, p.url, p.title, p.body_text,
          SUM(po.tf_idf)                                          AS tfidf_sum,
          COUNT(DISTINCT po.term_id)                              AS term_hits,
          CAST(COUNT(DISTINCT po.term_id) AS DECIMAL(10,4)) / ${numTerms} AS coverage,
          COALESCE(pr.score, 1.0)                                 AS page_rank,
          SUM(po.tf_idf)
            * (CAST(COUNT(DISTINCT po.term_id) AS DECIMAL(10,4)) / ${numTerms})
            * COALESCE(pr.score, 1.0)                             AS final_score
        FROM postings po
        JOIN pages p ON p.id = po.page_id
        LEFT JOIN page_rank pr ON pr.page_id = po.page_id
        WHERE po.term_id IN (${idPH})
        GROUP BY p.id
        ORDER BY final_score DESC
        LIMIT ? OFFSET ?
      `, [...termIds, limit, offset]);

      const results = rows.map(r => ({
        id:        r.id,
        url:       String(r.url || ""),
        title:     String(r.title || r.url || ""),
        snippet:   highlight(r.body_text, terms),
        score:     parseFloat(Number(r.final_score).toFixed(4)),
        term_hits: r.term_hits,
        coverage:  parseFloat(Number(r.coverage).toFixed(2)),
      }));

      return res.json({ query: rawQuery, total, page, limit, took_ms: Date.now() - start, results });

    } catch (err) {
      console.error("Search error:", err);
      return res.status(500).json({ error: "Search failed" });
    }
  });

  app.get("/suggest", async (req, res) => {
    const q = (req.query.q || "").trim().toLowerCase();
    if (q.length < 2) return res.json([]);
    try {
      const [rows] = await pool.query(
        `SELECT term FROM terms WHERE term LIKE ? AND doc_freq > 1 ORDER BY doc_freq DESC LIMIT 8`,
        [`${q}%`]
      );
      res.json(rows.map(r => r.term));
    } catch { res.json([]); }
  });

  app.get("/stats", async (req, res) => {
    try {
      const [rows] = await pool.query(`
        SELECT
          (SELECT COUNT(*) FROM pages)    AS pages,
          (SELECT COUNT(*) FROM terms)    AS terms,
          (SELECT COUNT(*) FROM postings) AS postings
      `);
      res.json(rows[0]);
    } catch (err) {
      res.status(500).json({ error: "Stats unavailable" });
    }
  });

async function start() {
  try {
    await pool.query("SELECT 1");
    console.log("Connected to MySQL");
  } catch (error) {
    console.error("ERROR: could not connect to MySQL");
    console.error(error.message);
    process.exit(1);
  }

  app.listen(PORT, () => {
    console.log(`\nSearch API  → http://localhost:${PORT}`);
    console.log(`Search UI   → http://localhost:${PORT}/index.html\n`);
  });
}

start();
