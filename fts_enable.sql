-- Enable FTS5 virtual tables for full‑text search over products and reviews.
-- Run in sqlite3 shell:  .read fts_enable.sql

-- Products FTS on name & description
CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING fts5(
    name, description, content='products', content_rowid='id'
);
INSERT INTO products_fts(rowid, name, description)
  SELECT id, name, description FROM products
  WHERE name IS NOT NULL OR description IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS products_ai AFTER INSERT ON products BEGIN
  INSERT INTO products_fts(rowid, name, description) VALUES (new.id, new.name, new.description);
END;
CREATE TRIGGER IF NOT EXISTS products_ad AFTER DELETE ON products BEGIN
  INSERT INTO products_fts(products_fts, rowid, name, description) VALUES('delete', old.id, old.name, old.description);
END;
CREATE TRIGGER IF NOT EXISTS products_au AFTER UPDATE ON products BEGIN
  INSERT INTO products_fts(products_fts, rowid, name, description) VALUES('delete', old.id, old.name, old.description);
  INSERT INTO products_fts(rowid, name, description) VALUES (new.id, new.name, new.description);
END;

-- Reviews FTS on body and author (with body weighted higher via column order if desired)
CREATE VIRTUAL TABLE IF NOT EXISTS reviews_fts USING fts5(
    body, author, content='reviews', content_rowid='id'
);
INSERT INTO reviews_fts(rowid, body, author)
  SELECT id, body, author FROM reviews
  WHERE body IS NOT NULL OR author IS NOT NULL;

CREATE TRIGGER IF NOT EXISTS reviews_ai AFTER INSERT ON reviews BEGIN
  INSERT INTO reviews_fts(rowid, body, author) VALUES (new.id, new.body, new.author);
END;
CREATE TRIGGER IF NOT EXISTS reviews_ad AFTER DELETE ON reviews BEGIN
  INSERT INTO reviews_fts(reviews_fts, rowid, body, author) VALUES('delete', old.id, old.body, old.author);
END;
CREATE TRIGGER IF NOT EXISTS reviews_au AFTER UPDATE ON reviews BEGIN
  INSERT INTO reviews_fts(reviews_fts, rowid, body, author) VALUES('delete', old.id, old.body, old.author);
  INSERT INTO reviews_fts(rowid, body, author) VALUES (new.id, new.body, new.author);
END;
