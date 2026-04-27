import unittest
from pathlib import Path
import sys

from bs4 import BeautifulSoup

sys.path.append(str(Path(__file__).resolve().parents[1] / "scripts" / "python"))

from crawler import extract_text, same_domain_links, wikipedia_article_url


class CrawlerHelperTests(unittest.TestCase):
    def test_wikipedia_article_url_filters_namespaces(self):
        self.assertEqual(
            wikipedia_article_url("https://en.wikipedia.org/wiki/Search_engine"),
            "https://en.wikipedia.org/wiki/Search_engine",
        )
        self.assertIsNone(wikipedia_article_url("https://en.wikipedia.org/wiki/Category:Search_engines"))

    def test_wikipedia_links_only_keep_article_paths(self):
        html = """
        <html><body>
            <a href="/wiki/Information_retrieval">good</a>
            <a href="/wiki/File:Example.jpg">bad</a>
            <a href="/w/index.php?title=Search">bad</a>
        </body></html>
        """
        soup = BeautifulSoup(html, "lxml")

        links = same_domain_links("https://en.wikipedia.org/wiki/Main_Page", soup)

        self.assertEqual(links, ["https://en.wikipedia.org/wiki/Information_retrieval"])

    def test_wikipedia_text_prefers_main_content(self):
        html = """
        <html>
          <body>
            <nav>navigation noise</nav>
            <div id="mw-content-text">
              <p>Relevant article text.</p>
              <div class="navbox">footer clutter</div>
            </div>
          </body>
        </html>
        """
        soup = BeautifulSoup(html, "lxml")

        text = extract_text(soup, "https://en.wikipedia.org/wiki/Information_retrieval")

        self.assertIn("Relevant article text.", text)
        self.assertNotIn("navigation noise", text)
        self.assertNotIn("footer clutter", text)


if __name__ == "__main__":
    unittest.main()
