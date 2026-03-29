Ranking

Completed:

- [x] Replaced TF-IDF-only ranking with BM25.
- [x] Added field boosting (title > body).
- [x] Replaced `page_rank = log(length)` with graph-based score from crawl link edges.

Next upgrades:

- [ ] Add phrase/proximity boost using stored term positions.
- [ ] Add query spelling correction and typo-tolerant ranking.
- [ ] Add scheduled incremental re-indexing workflow.
