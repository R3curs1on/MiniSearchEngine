// server.js — Express Search API (MySQL version)

const express = require("express");
const cors = require("cors");
const mysql = require("mysql2/promise");
const fs = require("fs");
const path = require("path");

function loadDotEnv() {
  /* 
  for loadinng .env file 
  used for connecting to mysql database without hardcoding credentials in code;
  it reads the .env file, parses key-value pairs, 
  and sets them as environment variables in process.env, 
  allowing us to access database credentials securely without exposing them in our source code
  */
  const envPath = path.join(__dirname, ".env");
  if (!fs.existsSync(envPath)) return;

  const lines = fs.readFileSync(envPath, "utf8").split(/\r?\n/);
  for (const rawLine of lines) {  // parse each line in .env file ; before '=' is key and after '=' is value
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
  // checking is required env variable is present or not ; if not present throw error with message to set credentials in .env file
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

// Create MySQL connection pool with credentials from environment variables; this allows us to manage multiple database connections efficiently,
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

function tokenize(query) { // this function takes a search query as input and processes it to extract meaningful search terms; it converts the query to lowercase, removes non-alphabetic characters, splits it into individual words, and filters out common stopwords and very short terms, returning an array of relevant search terms that can be used for querying the database
  return query
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .split(/\s+/)
    .filter(t => t.length >= 2 && !STOPWORDS.has(t));
}

function escapeHtml(text) {  // this function takes a string of text and replaces special characters with their corresponding HTML entities to prevent issues like HTML injection when displaying user-generated content on a web page; it ensures that characters like &, <, >, ", and ' are safely escaped so they are displayed as literal characters rather than being interpreted as HTML tags or attributes
  return String(text).replace(/[&<>"']/g, char => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  })[char]);
}

function highlight(text, terms, snippetLen = 220) { 

  // this function generates a highlighted snippet of text from a larger body of text based on search terms; it searches for the first occurrence of any of the search terms in the text, extracts a snippet around that occurrence, and wraps the matching terms in <mark> tags for highlighting; if no terms are found, it returns a truncated version of the text as a snippet without highlights

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
    const rawQuery = (req.query.q || "").trim();  // this line retrieves the search query from the request's query parameters, trims any leading or trailing whitespace, and stores it in the variable rawQuery for further processing in the search functionality
    const page = Math.max(1,  parseInt(req.query.page, 10) || 1);  // this line retrieves the page number from the request's query parameters, parses it as an integer, and ensures that it is at least 1; if the page parameter is not provided or is invalid, it defaults to 1, allowing for pagination of search results in the API
    const limit = Math.min(20, parseInt(req.query.limit, 10) || 10);  // this line retrieves the limit for the number of search results per page from the request's query parameters, parses it as an integer, and ensures that it does not exceed 20; if the limit parameter is not provided or is invalid, it defaults to 10, allowing clients to control how many search results they receive while preventing excessively large responses that could impact performance
    const offset = (page - 1) * limit;    // this line calculates the offset for the SQL query based on the current page number and the limit of results per page; it determines how many results to skip in the database query to retrieve the correct set of results for the requested page, enabling pagination in the search API

    if (!rawQuery) return res.json({ results: [], total: 0, query: rawQuery });

    const terms = tokenize(rawQuery);    // this line takes the raw search query, processes it through the tokenize function to extract meaningful search terms by converting to lowercase, removing non-alphabetic characters, splitting into words, and filtering out stopwords; the resulting array of relevant search terms is stored in the variable terms for use in querying the database for matching results
    if (!terms.length) return res.json({ results: [], total: 0, query: rawQuery });

    const start = Date.now();     // this line records the current timestamp in milliseconds at the start of the search operation, allowing us to calculate how long the search takes by comparing it with another timestamp recorded at the end of the search process; this is useful for performance monitoring and providing feedback on the search duration in the API response

    try {
      // Get term IDs

      const placeholders = terms.map(() => "?").join(","); // this line creates a string of placeholders for a SQL query based on the number of search terms; it generates a comma-separated list of "?" characters, where each "?" corresponds to a search term, allowing us to safely parameterize the SQL query when retrieving term IDs from the database without risking SQL injection vulnerabilities
      const [termRows] = await pool.query(     // this line executes a SQL query to retrieve the IDs and terms from the "terms" table in the database where the term matches any of the search terms provided; it uses the placeholders generated in the previous line to safely parameterize the query, passing the array of search terms as parameters, and stores the resulting rows in termRows for further processing in the search functionality
        `SELECT id, term FROM terms WHERE term IN (${placeholders})`,
        terms
      );

      // if no matching terms are found in the database, we can immediately return an empty result set without performing further queries, as there would be no relevant pages to retrieve; this optimization helps improve performance by avoiding unnecessary database operations when the search terms do not exist in the index
      if (!termRows.length)
        return res.json({ results: [], total: 0, query: rawQuery, took_ms: Date.now() - start });

      // Extract term IDs and prepare for IN clause 
      const termIds  = termRows.map(r => r.id);
      const idPH     = termIds.map(() => "?").join(",");  // this is id placeholders for sql query like "IN (?,?,?)" based on number of termIds; it generates a comma-separated list of "?" characters corresponding to the number of term IDs, allowing us to safely parameterize the SQL query when retrieving matching pages from the database without risking SQL injection vulnerabilities
      const numTerms = termIds.length;  

      // Total matched pages
      const [countRows] = await pool.query(
        `SELECT COUNT(DISTINCT page_id) AS total FROM postings WHERE term_id IN (${idPH})`,
        termIds
      );
      const total = countRows[0] ? countRows[0].total : 0;

      // Ranked results

      /*
      final query retrieves pages that match the search terms 
      and calculates a relevance score for each page based on the sum of TF-IDF scores for the matching terms,
      the coverage of search terms in the page, 
      and the page rank;
      it joins the postings, pages, 
      and page_rank tables to gather necessary data, groups results by page ID, 
      and orders them by the final relevance score in descending order, 
      allowing us to return a ranked list of search results with pagination support
      */
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


      /*
        restults are mapped to a format suitable for the API response, 
        where each result includes the 
        page ID,
         URL, 
         title,
          a highlighted snippet of the body text based on the search terms, 
          the calculated relevance score, the number of search terms that hit in the page, 
          and the coverage of search terms in the page; 
          this mapping prepares the raw database results for presentation in the API response, 
      */

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
    /*
    this is additional endpoint for search suggestions; 
    it takes a query parameter 'q', trims and converts it to lowercase
    , and if the length of the query is at least 2 characters, 
    it performs a SQL query to find terms in the database that start with the given query string 
    and have a document frequency greater than 1;
     the results are ordered by document frequency in descending order 
     and limited to 8 suggestions, which are then returned as a JSON array of terms;
      if any error occurs during the database query, an empty array is returned instead
    */
    const q = (req.query.q || "").trim().toLowerCase();

    if (q.length < 2) return res.json([]);
    try {
      /*
        actual query looks for terms in the "terms" table where the term starts with the provided query string (using the LIKE operator with a wildcard) and has a document frequency greater than 1;
         it orders the results by document frequency in descending order to prioritize more common terms and limits the results to 8 suggestions, which are then returned as a JSON array of term strings for use in search suggestions or autocomplete functionality in the search UI
      */
      const [rows] = await pool.query(
        `SELECT term FROM terms WHERE term LIKE ? AND doc_freq > 1 ORDER BY doc_freq DESC LIMIT 8`,
        [`${q}%`]
      );
      res.json(rows.map(r => r.term));
    } catch { res.json([]); }
  });

  app.get("/stats", async (req, res) => {

    /*
      this is a simple endpoint to retrieve statistics about the search index,
       including the total number of pages, terms, and postings in the database;
        it executes a SQL query that counts the total entries in the "pages", "terms", and "postings" tables and returns these counts as a JSON object
        ; if any error occurs during the database query, it responds with a 500 status code and an error message indicating that the stats are unavailable
    */
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
  /*
  main function to start the server; it first attempts to connect to the MySQL database by executing a simple query (SELECT 1) to verify the connection; if the connection is successful, it logs a success message and starts the Express server on the specified port, logging the URLs for the Search API and Search UI; if the connection fails, it logs an error message with details and exits the process with a non-zero status code to indicate failure
  */
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
