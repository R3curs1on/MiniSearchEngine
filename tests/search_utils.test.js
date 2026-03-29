const test = require("node:test");
const assert = require("node:assert/strict");

const { tokenize, highlight } = require("../search_utils");

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
