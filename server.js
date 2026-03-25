// server.js — Express Search API (sql.js version — pure JS, no compilation needed)

const express   = require("express");
const cors      = require("cors");
const initSqlJs = require("sql.js");
const fs        = require("fs");
const path      = require("path");

const app     = express();
const PORT    = process.env.PORT || 3001;
const DB_PATH = path.join(__dirname, "search_engine.db");

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

function highlight(text, terms, snippetLen = 220) {
  if (!text) return "";
  const escaped = terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const regex   = new RegExp(`(${escaped.join("|")})`, "gi");
  const match   = regex.exec(text);
  if (!match) return text.slice(0, snippetLen) + (text.length > snippetLen ? "…" : "");
  const start   = Math.max(0, match.index - 80);
  const end     = Math.min(text.length, start + snippetLen);
  const snippet = (start > 0 ? "…" : "") + text.slice(start, end) + (end < text.length ? "…" : "");
  return snippet.replace(new RegExp(`(${escaped.join("|")})`, "gi"), "<mark>$1</mark>");
}

// ─── sql.js query helper ─────────────────────────────────────────────────────
// Returns array of plain objects from a SELECT

function query(db, sql, params = []) {
  const stmt = db.prepare(sql);
  stmt.bind(params);
  const rows = [];
  while (stmt.step()) rows.push(stmt.getAsObject());
  stmt.free();
  return rows;
}

function queryOne(db, sql, params = []) {
  const rows = query(db, sql, params);
  return rows[0] || null;
}

// ─── Boot: load DB then start server ─────────────────────────────────────────

(async () => {
  if (!fs.existsSync(DB_PATH)) {
    console.error(`ERROR: search_engine.db not found at ${DB_PATH}`);
    console.error("Run crawler.py and indexer.py first.");
    process.exit(1);
  }

  const SQL     = await initSqlJs();
  const filebuf = fs.readFileSync(DB_PATH);
  const db      = new SQL.Database(filebuf);
  console.log("Loaded search_engine.db into memory");

  // ─── Routes ──────────────────────────────────────────────────────────────

  app.get("/search", (req, res) => {
    const rawQuery = (req.query.q || "").trim();
    const page     = Math.max(1,  parseInt(req.query.page)  || 1);
    const limit    = Math.min(20, parseInt(req.query.limit) || 10);
    const offset   = (page - 1) * limit;

    if (!rawQuery) return res.json({ results: [], total: 0, query: rawQuery });

    const terms = tokenize(rawQuery);
    if (!terms.length) return res.json({ results: [], total: 0, query: rawQuery });

    const start = Date.now();

    try {
      // Get term IDs
      const placeholders = terms.map(() => "?").join(",");
      const termRows = query(db,
        `SELECT id, term FROM terms WHERE term IN (${placeholders})`,
        terms
      );
      if (!termRows.length)
        return res.json({ results: [], total: 0, query: rawQuery, took_ms: Date.now() - start });

      const termIds  = termRows.map(r => r.id);
      const idPH     = termIds.map(() => "?").join(",");
      const numTerms = termIds.length;

      // Total matched pages
      const countRow = queryOne(db,
        `SELECT COUNT(DISTINCT page_id) AS total FROM postings WHERE term_id IN (${idPH})`,
        termIds
      );
      const total = countRow ? countRow.total : 0;

      // Ranked results
      const rows = query(db, `
        SELECT
          p.id, p.url, p.title, p.body_text,
          SUM(po.tf_idf)                                          AS tfidf_sum,
          COUNT(DISTINCT po.term_id)                              AS term_hits,
          CAST(COUNT(DISTINCT po.term_id) AS REAL) / ${numTerms} AS coverage,
          COALESCE(pr.score, 1.0)                                 AS page_rank,
          SUM(po.tf_idf)
            * (CAST(COUNT(DISTINCT po.term_id) AS REAL) / ${numTerms})
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
        url:       r.url,
        title:     r.title || r.url,
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

  app.get("/suggest", (req, res) => {
    const q = (req.query.q || "").trim().toLowerCase();
    if (q.length < 2) return res.json([]);
    try {
      const rows = query(db,
        `SELECT term FROM terms WHERE term LIKE ? AND doc_freq > 1 ORDER BY doc_freq DESC LIMIT 8`,
        [`${q}%`]
      );
      res.json(rows.map(r => r.term));
    } catch { res.json([]); }
  });

  app.get("/stats", (req, res) => {
    try {
      const row = queryOne(db, `
        SELECT
          (SELECT COUNT(*) FROM pages)    AS pages,
          (SELECT COUNT(*) FROM terms)    AS terms,
          (SELECT COUNT(*) FROM postings) AS postings
      `);
      res.json(row);
    } catch (err) {
      res.status(500).json({ error: "Stats unavailable" });
    }
  });

  // ─── Start ─────────────────────────────────────────────────────────────────
  app.listen(PORT, () => {
    console.log(`\nSearch API  → http://localhost:${PORT}`);
    console.log(`Search UI   → http://localhost:${PORT}/index.html\n`);
  });

})();
