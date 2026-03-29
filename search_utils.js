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

module.exports = {
  STOPWORDS,
  tokenize,
  escapeHtml,
  highlight,
};
