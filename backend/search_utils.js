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

const MAX_PREFIX_SUGGESTIONS = 12;

function tokenize(query) {
  return String(query || "")
    .toLowerCase()
    .replace(/[^a-z\s]/g, " ")
    .split(/\s+/)
    .filter((term) => term.length >= 2 && !STOPWORDS.has(term));
}

function escapeHtml(text) {
  return String(text).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  })[char]);
}

function highlight(text, terms, snippetLen = 220) {
  if (!text) return "";
  const escapedTerms = terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  if (!escapedTerms.length) {
    const baseSnippet = text.slice(0, snippetLen) + (text.length > snippetLen ? "..." : "");
    return escapeHtml(baseSnippet);
  }

  const regex = new RegExp(`(${escapedTerms.join("|")})`, "gi");
  const match = regex.exec(text);
  if (!match) {
    const baseSnippet = text.slice(0, snippetLen) + (text.length > snippetLen ? "..." : "");
    return escapeHtml(baseSnippet);
  }

  const start = Math.max(0, match.index - 80);
  const end = Math.min(text.length, start + snippetLen);
  const snippet = (start > 0 ? "..." : "") + text.slice(start, end) + (end < text.length ? "..." : "");
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

class TrieNode {
  constructor() {
    this.children = new Map();
    this.isWord = false;
    this.topTerms = [];
  }
}

class Trie {
  constructor(limit = MAX_PREFIX_SUGGESTIONS) {
    this.root = new TrieNode();
    this.limit = limit;
  }

  insert(term, weight) {
    let node = this.root;
    for (const char of term) {
      if (!node.children.has(char)) node.children.set(char, new TrieNode());
      node = node.children.get(char);
      updateTopTerms(node.topTerms, term, weight, this.limit);
    }
    node.isWord = true;
  }

  suggest(prefix, limit = 8) {
    let node = this.root;
    for (const char of prefix) {
      node = node.children.get(char);
      if (!node) return [];
    }
    return node.topTerms.slice(0, limit).map((entry) => entry.term);
  }
}

function updateTopTerms(list, term, weight, limit) {
  const existing = list.findIndex((entry) => entry.term === term);
  const nextEntry = { term, weight };
  if (existing >= 0) {
    list[existing] = nextEntry;
  } else {
    list.push(nextEntry);
  }

  list.sort((a, b) => {
    if (b.weight !== a.weight) return b.weight - a.weight;
    return a.term.localeCompare(b.term);
  });

  if (list.length > limit) list.length = limit;
}

function buildTermKeys(term) {
  const keys = new Set();
  const padded = `^${term}$`;
  const sizes = term.length <= 4 ? [2] : [2, 3];

  for (const size of sizes) {
    for (let i = 0; i <= padded.length - size; i += 1) {
      keys.add(padded.slice(i, i + size));
    }
  }

  return [...keys];
}

function buildLexicon(rows) {
  const trie = new Trie();
  const terms = new Set();
  const byTerm = new Map();
  const gramIndex = new Map();

  for (const row of rows || []) {
    const term = String(row.term || "").toLowerCase();
    if (!term) continue;
    const docFreq = Number(row.doc_freq || 0);

    terms.add(term);
    byTerm.set(term, { term, docFreq });
    trie.insert(term, docFreq);

    for (const key of buildTermKeys(term)) {
      if (!gramIndex.has(key)) gramIndex.set(key, []);
      gramIndex.get(key).push(term);
    }
  }

  return {
    trie,
    terms,
    byTerm,
    gramIndex,
  };
}

function maxEditDistance(length) {
  if (length <= 4) return 1;
  if (length <= 8) return 2;
  return 3;
}

function damerauLevenshtein(a, b, maxDistance = Infinity) {
  if (a === b) return 0;
  if (!a.length) return b.length;
  if (!b.length) return a.length;
  if (Math.abs(a.length - b.length) > maxDistance) return maxDistance + 1;

  const rows = a.length + 2;
  const cols = b.length + 2;
  const maxDist = a.length + b.length;
  const matrix = Array.from({ length: rows }, () => Array(cols).fill(0));
  const seen = Object.create(null);

  matrix[0][0] = maxDist;
  for (let i = 0; i <= a.length; i += 1) {
    matrix[i + 1][0] = maxDist;
    matrix[i + 1][1] = i;
  }
  for (let j = 0; j <= b.length; j += 1) {
    matrix[0][j + 1] = maxDist;
    matrix[1][j + 1] = j;
  }

  for (let i = 1; i <= a.length; i += 1) {
    let db = 0;
    let rowMin = Infinity;

    for (let j = 1; j <= b.length; j += 1) {
      const i1 = seen[b[j - 1]] || 0;
      const j1 = db;
      let cost = 1;

      if (a[i - 1] === b[j - 1]) {
        cost = 0;
        db = j;
      }

      matrix[i + 1][j + 1] = Math.min(
        matrix[i][j] + cost,
        matrix[i + 1][j] + 1,
        matrix[i][j + 1] + 1,
        matrix[i1][j1] + (i - i1 - 1) + 1 + (j - j1 - 1)
      );

      rowMin = Math.min(rowMin, matrix[i + 1][j + 1]);
    }

    if (rowMin > maxDistance) return maxDistance + 1;
    seen[a[i - 1]] = i;
  }

  return matrix[a.length + 1][b.length + 1];
}

function pickCorrection(term, lexicon) {
  if (!lexicon || lexicon.terms.has(term) || term.length < 3) return null;

  const maxDistance = maxEditDistance(term.length);
  const keyCounts = new Map();
  const candidateTerms = new Set();

  for (const key of buildTermKeys(term)) {
    for (const candidate of lexicon.gramIndex.get(key) || []) {
      keyCounts.set(candidate, (keyCounts.get(candidate) || 0) + 1);
      candidateTerms.add(candidate);
    }
  }

  if (!candidateTerms.size) {
    for (const candidate of lexicon.trie.suggest(term.slice(0, 1), MAX_PREFIX_SUGGESTIONS)) {
      candidateTerms.add(candidate);
    }
  }

  let best = null;

  for (const candidate of candidateTerms) {
    if (Math.abs(candidate.length - term.length) > maxDistance) continue;

    const distance = damerauLevenshtein(term, candidate, maxDistance);
    if (distance > maxDistance) continue;

    const overlap = keyCounts.get(candidate) || 0;
    const overlapRatio = overlap / Math.max(buildTermKeys(term).length, 1);
    const docFreq = lexicon.byTerm.get(candidate)?.docFreq || 0;
    const prefixBonus = candidate[0] === term[0] ? 0.25 : 0;
    const score = (overlapRatio * 2.2) + prefixBonus + (Math.log1p(docFreq) * 0.08) - (distance * 0.95);

    if (!best || score > best.score || (score === best.score && docFreq > best.docFreq)) {
      best = { term: candidate, distance, docFreq, score };
    }
  }

  if (!best || best.score < 0.15) return null;
  return best;
}

function correctTerms(terms, lexicon) {
  const corrections = [];
  const nextTerms = [];

  for (const term of terms) {
    const correction = pickCorrection(term, lexicon);
    if (correction && correction.term !== term) {
      nextTerms.push(correction.term);
      corrections.push({
        from: term,
        to: correction.term,
        distance: correction.distance,
      });
      continue;
    }
    nextTerms.push(term);
  }

  return {
    terms: nextTerms,
    applied: corrections.length > 0,
    corrections,
    correctedQuery: nextTerms.join(" "),
  };
}

function hasAdjacentPair(left, right) {
  let i = 0;
  let j = 0;

  while (i < left.length && j < right.length) {
    const diff = right[j] - left[i];
    if (diff === 1) return true;
    if (diff <= 0) {
      j += 1;
    } else {
      i += 1;
    }
  }

  return false;
}

function hasExactPhrase(lists) {
  if (!lists.length || lists.some((positions) => !positions.length)) return false;
  const lookup = lists.map((positions) => new Set(positions));

  for (const start of lists[0]) {
    let matches = true;
    for (let i = 1; i < lookup.length; i += 1) {
      if (!lookup[i].has(start + i)) {
        matches = false;
        break;
      }
    }
    if (matches) return true;
  }

  return false;
}

function smallestCoveringSpan(lists) {
  const merged = [];
  lists.forEach((positions, index) => {
    positions.forEach((position) => {
      merged.push({ position, index });
    });
  });

  if (!merged.length) return Infinity;
  merged.sort((a, b) => a.position - b.position);

  const counts = new Map();
  let covered = 0;
  let left = 0;
  let best = Infinity;

  for (let right = 0; right < merged.length; right += 1) {
    const { index } = merged[right];
    counts.set(index, (counts.get(index) || 0) + 1);
    if (counts.get(index) === 1) covered += 1;

    while (covered === lists.length && left <= right) {
      best = Math.min(best, merged[right].position - merged[left].position);
      const leftIndex = merged[left].index;
      counts.set(leftIndex, counts.get(leftIndex) - 1);
      if (counts.get(leftIndex) === 0) covered -= 1;
      left += 1;
    }
  }

  return best;
}

function computeFieldSignals(queryTerms, positionsByTerm, field) {
  const lists = queryTerms.map((term) => positionsByTerm[term]?.[field] || []);
  if (lists.some((positions) => positions.length === 0)) {
    return { pairRatio: 0, fullPhrase: 0, proximity: 0 };
  }

  const pairTotal = Math.max(queryTerms.length - 1, 0);
  const pairHits = pairTotal
    ? lists.slice(0, -1).reduce((sum, positions, index) => (
      sum + (hasAdjacentPair(positions, lists[index + 1]) ? 1 : 0)
    ), 0)
    : 0;

  const fullPhrase = hasExactPhrase(lists) ? 1 : 0;
  const span = smallestCoveringSpan(lists);
  const idealSpan = Math.max(queryTerms.length - 1, 0);
  const extraSpan = Number.isFinite(span) ? Math.max(0, span - idealSpan) : Infinity;
  const proximity = Number.isFinite(extraSpan) ? 1 / (1 + extraSpan) : 0;

  return {
    pairRatio: pairTotal ? pairHits / pairTotal : 0,
    fullPhrase,
    proximity,
  };
}

function computePositionalSignals(queryTerms, positionsByTerm) {
  if (!queryTerms || queryTerms.length < 2) {
    return {
      phraseScore: 0,
      proximityScore: 0,
    };
  }

  const title = computeFieldSignals(queryTerms, positionsByTerm, "title");
  const body = computeFieldSignals(queryTerms, positionsByTerm, "body");

  const phraseScore = Number((
    (title.pairRatio * 1.4) +
    body.pairRatio +
    (title.fullPhrase * 1.8) +
    (body.fullPhrase * 1.25)
  ).toFixed(4));

  const proximityScore = Number((
    (title.proximity * 1.15) +
    body.proximity
  ).toFixed(4));

  return {
    phraseScore,
    proximityScore,
  };
}

module.exports = {
  STOPWORDS,
  tokenize,
  escapeHtml,
  highlight,
  Trie,
  buildLexicon,
  damerauLevenshtein,
  correctTerms,
  computePositionalSignals,
};
