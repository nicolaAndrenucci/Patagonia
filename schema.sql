PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS products(
  id INTEGER PRIMARY KEY,
  source_domain TEXT,
  url TEXT UNIQUE,
  sku TEXT,
  name TEXT,
  brand TEXT,
  description TEXT,
  category TEXT,
  images TEXT,
  materials TEXT,
  created_at TEXT,
  updated_at TEXT
);

CREATE TABLE IF NOT EXISTS variants(
  id INTEGER PRIMARY KEY,
  product_id INTEGER,
  variant_sku TEXT,
  color TEXT,
  size TEXT,
  upc TEXT, ean TEXT, gtin TEXT,
  price REAL, currency TEXT, availability TEXT,
  raw TEXT,
  FOREIGN KEY(product_id) REFERENCES products(id)
);

CREATE TABLE IF NOT EXISTS reviews(
  id INTEGER PRIMARY KEY,
  product_id INTEGER,
  rating REAL,
  title TEXT,
  body TEXT,
  author TEXT,
  lang TEXT,
  published_at TEXT,
  source TEXT,
  raw TEXT,
  unique_hash TEXT UNIQUE,
  FOREIGN KEY(product_id) REFERENCES products(id)
);

-- Helpful indices
CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);
CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
