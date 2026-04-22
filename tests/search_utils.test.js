const test = require("node:test");
const assert = require("node:assert/strict");

const {
  tokenize,
  highlight,
  buildLexicon,
  correctTerms,
  computePositionalSignals,
} = require("../backend/search_utils");

test("tokenize strips stopwords and punctuation", () => {
  const terms = tokenize("The Quick, Brown fox jumps in the river!");
  assert.deepEqual(terms, ["quick", "brown", "fox", "jumps", "river"]);
});

test("tokenize ignores one-letter words and numbers", () => {
  const terms = tokenize("a b c JavaScript 101 BM25");
  assert.deepEqual(terms, ["javascript", "bm"]);
});

test("highlight marks matched terms and escapes html", () => {
  const html = highlight("alpha <script>x</script> beta", ["beta"]);
  assert.ok(html.includes("<mark>beta</mark>"));
  assert.ok(!html.includes("<script>"));
});

test("lexicon-backed correction fixes close misspellings", () => {
  const lexicon = buildLexicon([
    { term: "python", doc_freq: 42 },
    { term: "pytest", doc_freq: 18 },
    { term: "typhoon", doc_freq: 7 },
  ]);

  const corrected = correctTerms(["pythn", "pyest"], lexicon);

  assert.equal(corrected.applied, true);
  assert.deepEqual(corrected.terms, ["python", "pytest"]);
  assert.equal(corrected.correctedQuery, "python pytest");
});

test("trie suggestions return the strongest prefix matches", () => {
  const lexicon = buildLexicon([
    { term: "python", doc_freq: 50 },
    { term: "pytest", doc_freq: 30 },
    { term: "pyramid", doc_freq: 12 },
    { term: "pycharm", doc_freq: 9 },
  ]);

  assert.deepEqual(lexicon.trie.suggest("py", 3), ["python", "pytest", "pyramid"]);
});

test("positional signals reward phrase and proximity matches", () => {
  const strong = computePositionalSignals(["machine", "learning"], {
    machine: { title: [0], body: [10, 40] },
    learning: { title: [1], body: [11, 41] },
  });
  const weak = computePositionalSignals(["machine", "learning"], {
    machine: { title: [], body: [10] },
    learning: { title: [], body: [28] },
  });

  assert.ok(strong.phraseScore > weak.phraseScore);
  assert.ok(strong.proximityScore > weak.proximityScore);
});
