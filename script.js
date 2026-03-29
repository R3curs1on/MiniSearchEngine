const API = window.location.origin && window.location.origin !== "null"
  ? window.location.origin
  : "http://localhost:3001";

const HISTORY_KEY = "mini_search_history_v1";
const HISTORY_LIMIT = 8;

let currentPage = 1;
let currentQuery = "";
let suggestTimer = null;

const qEl = document.getElementById("q");
const heroEl = document.getElementById("hero");
const metaEl = document.getElementById("meta");
const listEl = document.getElementById("results-list");
const loadingEl = document.getElementById("loading");
const emptyEl = document.getElementById("empty");
const paginEl = document.getElementById("pagination");
const suggestBox = document.getElementById("suggest-box");
const statsBar = document.getElementById("stats-bar");
const indexStats = document.getElementById("index-stats");
const badgeRow = document.getElementById("badge-row");
const historyList = document.getElementById("history-list");
const metricsBox = document.getElementById("metrics-box");

function clearChildren(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

function prettyNum(value) {
  return Number(value || 0).toLocaleString();
}

function renderBadge(label, value) {
  const badge = document.createElement("span");
  badge.className = "badge";
  const key = document.createElement("strong");
  key.textContent = `${label}: `;
  badge.appendChild(key);
  badge.appendChild(document.createTextNode(value));
  return badge;
}

function setBadges(items) {
  clearChildren(badgeRow);
  if (!items.length) {
    badgeRow.style.display = "none";
    return;
  }
  items.forEach((item) => badgeRow.appendChild(renderBadge(item.label, item.value)));
  badgeRow.style.display = "flex";
}

function loadQueryHistory() {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    const parsed = JSON.parse(raw || "[]");
    return Array.isArray(parsed) ? parsed.filter(Boolean) : [];
  } catch {
    return [];
  }
}

function saveQueryHistory(query) {
  const next = [query, ...loadQueryHistory().filter((q) => q !== query)].slice(0, HISTORY_LIMIT);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(next));
  renderHistory();
}

function renderHistory() {
  const history = loadQueryHistory();
  clearChildren(historyList);
  if (!history.length) {
    historyList.textContent = "No recent queries yet.";
    historyList.className = "is-empty";
    return;
  }
  historyList.className = "";
  history.forEach((query) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "history-item";
    button.textContent = query;
    button.addEventListener("click", () => {
      qEl.value = query;
      doSearch(query, 1);
    });
    historyList.appendChild(button);
  });
}

async function loadStats() {
  try {
    const res = await fetch(`${API}/stats`);
    if (!res.ok) throw new Error("stats unavailable");
    const data = await res.json();

    statsBar.textContent =
      `${prettyNum(data.pages)} pages · ${prettyNum(data.terms)} terms · ${prettyNum(data.postings)} postings indexed`;
    statsBar.classList.add("visible");

    indexStats.innerHTML = `
      <p class="section-label">Index snapshot</p>
      <div class="stat-grid">
        <div class="stat-card"><div class="stat-num">${prettyNum(data.pages)}</div><div class="stat-lbl">Pages</div></div>
        <div class="stat-card"><div class="stat-num">${prettyNum(data.terms)}</div><div class="stat-lbl">Unique Terms</div></div>
        <div class="stat-card"><div class="stat-num">${prettyNum(data.postings)}</div><div class="stat-lbl">Postings</div></div>
      </div>
    `;
    indexStats.style.display = "block";

    setBadges([
      { label: "Ranking", value: data.ranking_strategy || "n/a" },
      { label: "Avg doc len", value: Number(data.avg_doc_len || 0).toFixed(1) },
    ]);
  } catch {
    statsBar.textContent = "Connect the API to see index stats";
    statsBar.classList.add("visible");
  }
}

async function loadMetrics() {
  try {
    const res = await fetch(`${API}/metrics`);
    if (!res.ok) throw new Error("metrics unavailable");
    const data = await res.json();
    metricsBox.innerHTML = `
      <div class="metric-row"><span>Uptime</span><strong>${Number(data.uptime_s || 0).toFixed(1)}s</strong></div>
      <div class="metric-row"><span>Crawl success</span><strong>${prettyNum(data.crawl_success)} / ${prettyNum(data.crawl_attempts)}</strong></div>
      <div class="metric-row"><span>Avg page rank</span><strong>${Number(data.avg_page_rank || 0).toFixed(4)}</strong></div>
      <div class="metric-row"><span>Last crawl</span><strong>${data.last_crawled_at ? new Date(data.last_crawled_at).toLocaleString() : "n/a"}</strong></div>
    `;
  } catch {
    metricsBox.textContent = "Metrics unavailable.";
  }
}

qEl.addEventListener("input", () => {
  clearTimeout(suggestTimer);
  const val = qEl.value.trim();
  if (val.length < 2) {
    suggestBox.style.display = "none";
    return;
  }

  suggestTimer = setTimeout(async () => {
    try {
      const res = await fetch(`${API}/suggest?q=${encodeURIComponent(val)}`);
      const terms = await res.json();
      if (!terms.length) {
        suggestBox.style.display = "none";
        return;
      }

      clearChildren(suggestBox);
      const heading = document.createElement("div");
      heading.className = "suggest-heading";
      heading.textContent = "Suggestions";
      suggestBox.appendChild(heading);

      terms.forEach((term) => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "suggest-item";
        item.textContent = term;
        item.addEventListener("click", () => pickSuggest(term));
        suggestBox.appendChild(item);
      });

      suggestBox.style.display = "block";
    } catch {
      suggestBox.style.display = "none";
    }
  }, 180);
});

function pickSuggest(term) {
  qEl.value = term;
  suggestBox.style.display = "none";
  doSearch(term, 1);
}

document.addEventListener("click", (event) => {
  if (!suggestBox.contains(event.target) && event.target !== qEl) {
    suggestBox.style.display = "none";
  }
});

document.getElementById("btn-search").addEventListener("click", () => {
  doSearch(qEl.value.trim(), 1);
});

qEl.addEventListener("keydown", (event) => {
  if (event.key === "Enter") doSearch(qEl.value.trim(), 1);
});

async function doSearch(query, page) {
  if (!query) return;
  currentQuery = query;
  currentPage = page;
  saveQueryHistory(query);

  suggestBox.style.display = "none";
  heroEl.classList.add("shrunk");
  indexStats.style.display = "none";
  loadingEl.style.display = "block";
  emptyEl.style.display = "none";
  metaEl.style.display = "none";
  clearChildren(listEl);
  clearChildren(paginEl);

  try {
    const res = await fetch(`${API}/search?q=${encodeURIComponent(query)}&page=${page}&limit=10`);
    const data = await res.json();
    loadingEl.style.display = "none";

    if (!data.results || data.results.length === 0) {
      emptyEl.style.display = "block";
      setBadges([
        { label: "Ranking", value: data.diagnostics?.strategy || "n/a" },
        { label: "Matched terms", value: "0" },
      ]);
      return;
    }

    const strategy = data.diagnostics?.strategy || "n/a";
    const matchedTerms = (data.diagnostics?.matched_terms || []).join(", ") || "none";

    metaEl.innerHTML = `
      About <strong>${prettyNum(data.total)}</strong> results
      &nbsp;·&nbsp; ${data.took_ms}ms
      &nbsp;·&nbsp; <code>${strategy}</code>
    `;
    metaEl.style.display = "block";

    setBadges([
      { label: "Ranking", value: strategy },
      { label: "Matched terms", value: matchedTerms },
      { label: "K1/B", value: `${data.diagnostics?.params?.k1 ?? "-"} / ${data.diagnostics?.params?.b ?? "-"}` },
      { label: "Title boost", value: `${data.diagnostics?.params?.title_boost ?? "-"}` },
    ]);

    data.results.forEach((result, index) => {
      listEl.appendChild(createResultCard(result, index));
    });

    renderPagination(data.total, data.limit, page);
    loadMetrics();
  } catch {
    loadingEl.style.display = "none";
    clearChildren(listEl);
    const error = document.createElement("p");
    error.className = "inline-error";
    error.textContent = "Could not reach API. Is server.js running?";
    listEl.appendChild(error);
  }
}

function createResultCard(result, index) {
  const card = document.createElement("div");
  card.className = "result-card";
  card.style.animationDelay = `${index * 35}ms`;

  const url = document.createElement("div");
  url.className = "result-url";
  url.textContent = result.url;

  const title = document.createElement("div");
  title.className = "result-title";
  const link = document.createElement("a");
  link.href = result.url;
  link.target = "_blank";
  link.rel = "noopener";
  link.textContent = result.title;
  title.appendChild(link);

  const snippet = document.createElement("div");
  snippet.className = "result-snippet";
  snippet.innerHTML = result.snippet;

  const meta = document.createElement("div");
  meta.className = "result-meta";
  [
    `score ${result.score}`,
    `terms ${result.term_hits}`,
    `coverage ${(Number(result.coverage || 0) * 100).toFixed(0)}%`,
  ].forEach((text) => {
    const span = document.createElement("span");
    span.textContent = text;
    meta.appendChild(span);
  });

  const signalWrap = document.createElement("div");
  signalWrap.className = "signal-wrap";
  [
    { key: "BM25", value: result.signals?.bm25 },
    { key: "PageRank", value: result.signals?.page_rank },
    { key: "Coverage", value: result.signals?.coverage },
  ].forEach((item) => {
    const signal = document.createElement("span");
    signal.className = "signal-chip";
    signal.textContent = `${item.key}: ${Number(item.value || 0).toFixed(4)}`;
    signalWrap.appendChild(signal);
  });

  card.appendChild(url);
  card.appendChild(title);
  card.appendChild(snippet);
  card.appendChild(meta);
  card.appendChild(signalWrap);
  return card;
}

function renderPagination(total, limit, current) {
  const totalPages = Math.ceil(total / limit);
  if (totalPages <= 1) return;
  const range = [];
  for (let p = Math.max(1, current - 2); p <= Math.min(totalPages, current + 2); p += 1) {
    range.push(p);
  }
  clearChildren(paginEl);
  range.forEach((pageNum) => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `page-btn ${pageNum === current ? "active" : ""}`;
    button.textContent = String(pageNum);
    button.addEventListener("click", () => doSearch(currentQuery, pageNum));
    paginEl.appendChild(button);
  });
}

renderHistory();
loadStats();
loadMetrics();
