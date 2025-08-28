-- Count products and reviews
SELECT (SELECT COUNT(*) FROM products) AS n_products,
       (SELECT COUNT(*) FROM reviews)  AS n_reviews;

-- Top-rated products by average review (requires reviews collected)
SELECT p.id, p.name, AVG(r.rating) AS avg_rating, COUNT(*) AS n
FROM reviews r
JOIN products p ON p.id = r.product_id
WHERE r.rating IS NOT NULL
GROUP BY p.id
HAVING n >= 3
ORDER BY avg_rating DESC, n DESC
LIMIT 20;

-- Find jackets with "GORE-TEX" in description (after enabling FTS)
SELECT p.id, p.name
FROM products_fts
JOIN products p ON p.id = products_fts.rowid
WHERE products_fts MATCH 'GORE NEAR/5 TEX';
