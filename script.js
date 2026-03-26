
const API = window.location.origin && window.location.origin !== "null"
  ? window.location.origin
  : "http://localhost:3001";
let currentPage = 1;
let currentQuery = "";
let suggestTimer = null;

const qEl         = document.getElementById("q");
const heroEl      = document.getElementById("hero");
const metaEl      = document.getElementById("meta");
const listEl      = document.getElementById("results-list");
const loadingEl   = document.getElementById("loading");
const emptyEl     = document.getElementById("empty");
const paginEl     = document.getElementById("pagination");
const suggestBox  = document.getElementById("suggest-box");
const statsBar    = document.getElementById("stats-bar");
const indexStats  = document.getElementById("index-stats");

function clearChildren(node) {
  while (node.firstChild) node.removeChild(node.firstChild);
}

// ── Load index stats on initial page ─────────────────────────────────────────
async function loadStats() {
  try {
    const res  = await fetch(`${API}/stats`);
    const data = await res.json();
    statsBar.textContent =
      `${data.pages.toLocaleString()} pages · ${data.terms.toLocaleString()} terms · ${data.postings.toLocaleString()} postings indexed`;
    statsBar.classList.add("visible");

    indexStats.innerHTML = `
      <p style="font-size:13px;color:var(--muted);margin-bottom:0.5rem;">Index statistics</p>
      <div class="stat-grid">
        <div class="stat-card"><div class="stat-num">${data.pages.toLocaleString()}</div><div class="stat-lbl">Pages</div></div>
        <div class="stat-card"><div class="stat-num">${data.terms.toLocaleString()}</div><div class="stat-lbl">Unique Terms</div></div>
        <div class="stat-card"><div class="stat-num">${data.postings.toLocaleString()}</div><div class="stat-lbl">Postings</div></div>
      </div>
    `;
  } catch { statsBar.textContent = "Connect the API to see stats"; statsBar.classList.add("visible"); }
}

// ── Autocomplete ──────────────────────────────────────────────────────────────
qEl.addEventListener("input", () => {
  clearTimeout(suggestTimer);
  const val = qEl.value.trim();
  if (val.length < 2) { suggestBox.style.display = "none"; return; }
  suggestTimer = setTimeout(async () => {
    try {
      const res   = await fetch(`${API}/suggest?q=${encodeURIComponent(val)}`);
      const terms = await res.json();
      if (!terms.length) { suggestBox.style.display = "none"; return; }
      clearChildren(suggestBox);
      terms.forEach(term => {
        const item = document.createElement("button");
        item.type = "button";
        item.className = "suggest-item";
        item.textContent = term;
        item.addEventListener("click", () => pickSuggest(term));
        suggestBox.appendChild(item);
      });
      suggestBox.style.display = "block";
    } catch { suggestBox.style.display = "none"; }
  }, 200);
});

function pickSuggest(term) {
  qEl.value = term;
  suggestBox.style.display = "none";
  doSearch(term, 1);
}

document.addEventListener("click", e => {
  if (!suggestBox.contains(e.target) && e.target !== qEl) suggestBox.style.display = "none";
});

// ── Search ────────────────────────────────────────────────────────────────────
document.getElementById("btn-search").addEventListener("click", () => {
  doSearch(qEl.value.trim(), 1);
});
qEl.addEventListener("keydown", e => {
  if (e.key === "Enter") doSearch(qEl.value.trim(), 1);
});

async function doSearch(query, page) {
  if (!query) return;
  currentQuery = query;
  currentPage  = page;
  suggestBox.style.display = "none";

  heroEl.classList.add("shrunk");
  indexStats.style.display = "none";
  loadingEl.style.display  = "block";
  clearChildren(listEl);
  metaEl.style.display     = "none";
  emptyEl.style.display    = "none";
  clearChildren(paginEl);

  try {
    const res  = await fetch(`${API}/search?q=${encodeURIComponent(query)}&page=${page}&limit=10`);
    const data = await res.json();
    loadingEl.style.display = "none";

    if (!data.results || data.results.length === 0) {
      emptyEl.style.display = "block"; return;
    }

    metaEl.innerHTML = `About <strong>${data.total.toLocaleString()}</strong> results &nbsp;·&nbsp; ${data.took_ms}ms`;
    metaEl.style.display = "block";

    clearChildren(listEl);
    data.results.forEach((result, i) => {
      listEl.appendChild(createResultCard(result, i));
    });

    renderPagination(data.total, data.limit, page);

  } catch {
    loadingEl.style.display = "none";
    clearChildren(listEl);
    const error = document.createElement("p");
    error.style.color = "var(--muted)";
    error.style.padding = "2rem 0";
    error.textContent = "Could not reach API. Is server.js running?";
    listEl.appendChild(error);
  }
}

function createResultCard(result, index) {
  const card = document.createElement("div");
  card.className = "result-card";
  card.style.animationDelay = `${index * 40}ms`;

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
    `score: ${result.score}`,
    `coverage: ${(result.coverage * 100).toFixed(0)}%`,
    `terms matched: ${result.term_hits}`,
  ].forEach(text => {
    const span = document.createElement("span");
    span.textContent = text;
    meta.appendChild(span);
  });

  card.appendChild(url);
  card.appendChild(title);
  card.appendChild(snippet);
  card.appendChild(meta);
  return card;
}

function renderPagination(total, limit, current) {
  const totalPages = Math.ceil(total / limit);
  if (totalPages <= 1) return;
  const range = [];
  for (let p = Math.max(1, current - 2); p <= Math.min(totalPages, current + 2); p++) range.push(p);
  clearChildren(paginEl);
  range.forEach(pageNum => {
    const button = document.createElement("button");
    button.type = "button";
    button.className = `page-btn ${pageNum === current ? "active" : ""}`;
    button.textContent = pageNum;
    button.addEventListener("click", () => doSearch(currentQuery, pageNum));
    paginEl.appendChild(button);
  });
}

// ── Init ──────────────────────────────────────────────────────────────────────
loadStats();
indexStats.style.display = "block";