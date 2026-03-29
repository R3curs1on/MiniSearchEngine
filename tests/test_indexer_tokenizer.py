import unittest

from indexer import tokenize


class TokenizeTests(unittest.TestCase):
    def test_stopwords_are_removed(self):
        tokens = tokenize("The quick brown fox jumps over the lazy dog")
        self.assertIn("quick", tokens)
        self.assertNotIn("the", tokens)

    def test_positions_are_capped(self):
        text = "alpha " * 120
        tokens = tokenize(text)
        self.assertEqual(len(tokens["alpha"]), 50)

    def test_only_alpha_terms_len_two_plus(self):
        tokens = tokenize("a b c go! 12 npm")
        self.assertIn("go", tokens)
        self.assertIn("npm", tokens)
        self.assertNotIn("a", tokens)
        self.assertNotIn("b", tokens)


if __name__ == "__main__":
    unittest.main()
