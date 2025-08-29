<<<<<<< HEAD
# Patagonia Products & Reviews — SQLite Scraper (Starter)

This starter collects product metadata (via JSON-LD) and any in-page `schema.org/Review` from Patagonia product URLs discovered through sitemaps, and stores them in **SQLite**.

> ⚠️ Please respect robots.txt and site Terms of Use. Use this for personal/educational purposes unless you have permission to crawl.

## Quick start

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate
source .venv/bin/activate
pip install -r requirements.txt

# edit BASE_DOMAIN in patagonia_scraper.py if needed (e.g., www.patagonia.com, eu.patagonia.com, etc.)
python patagonia_scraper.py
```

The script will create `patagonia.db` in the project directory.

## What’s inside

- `patagonia_scraper.py` — async Python scraper
- `requirements.txt` — dependencies
- `schema.sql` — DB schema & indices for SQLite
- `fts_enable.sql` — optional FTS5 virtual tables and triggers for full‑text search
- `export_csv.py` — export tables to CSV
- `sample_queries.sql` — useful example queries

## Enable Full‑Text Search (optional)

SQLite FTS5 requires loading the extension in some environments. If supported, run:

```bash
sqlite3 patagonia.db < fts_enable.sql
```

Then you can try queries like:

```sql
SELECT p.name, r.rating, snippet(reviews_fts, 1, '[', ']', '…', 10) AS snip
FROM reviews_fts
JOIN reviews r ON r.id = reviews_fts.rowid
JOIN products p ON p.id = r.product_id
WHERE reviews_fts MATCH 'caldo OR "very warm"'
ORDER BY r.rating DESC
LIMIT 20;
```

## Notes

- The starter extracts product info primarily from **JSON‑LD** and basic reviews from in‑page **schema.org**. Many e‑commerce sites load reviews through providers (e.g., Bazaarvoice, Yotpo). Add a module to query the public "display" endpoints if they exist (and abide by their TOS).
- Rate limits and polite crawling are built in; you can tune concurrency, delay, and URL limits.
- For persistent/hosted usage, you can wrap the SQLite DB with a lightweight FastAPI service (not included in this starter).
=======
# Patagonia
>>>>>>> fba448ad80fa3e929e74c2d7e398d7b9aff3a108
