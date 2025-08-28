import asyncio, re, json, hashlib
from urllib.parse import urljoin, urlparse
import httpx
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

# ---------- CONFIG ----------
BASE_DOMAIN = "www.patagonia.com"  # change for other locales (e.g., eu.patagonia.com)
BASE_URL = f"https://{BASE_DOMAIN}/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PatagoniaProductsCollector/1.3; contact: you@example.com)"}
CONCURRENCY = 5
RATE_DELAY = 0.5  # seconds between requests (per task)
SITEMAP_HINTS = ["sitemap.xml", "sitemap_index.xml", "sitemap-index.xml"]
PRODUCT_PATH_RE = re.compile(r"/(product|products|p)/", re.I)
MAX_URLS_PER_SITEMAP = 5000  # safety cap
LIMIT_FIRST_N_URLS = 200     # set None to scrape all discovered product-like URLs

DB_PATH = "patagonia.db"

# choose parser
try:
    import lxml  # noqa
    _PARSER = "lxml"
except Exception:
    _PARSER = "html.parser"

def now_iso():
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"

# ---------- DB (self-contained schema) ----------
def init_db(path=DB_PATH):
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.executescript("""
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
    CREATE TABLE IF NOT EXISTS materials(
      id INTEGER PRIMARY KEY,
      name TEXT UNIQUE
    );
    CREATE TABLE IF NOT EXISTS product_materials(
      id INTEGER PRIMARY KEY,
      product_id INTEGER NOT NULL,
      material_id INTEGER NOT NULL,
      percentage REAL,
      source TEXT NOT NULL DEFAULT '',
      raw TEXT NOT NULL DEFAULT '',
      UNIQUE(product_id, material_id, source, raw),
      FOREIGN KEY(product_id) REFERENCES products(id),
      FOREIGN KEY(material_id) REFERENCES materials(id)
    );
    """)
    # helpful indices
    cur.executescript("""
    CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
    CREATE INDEX IF NOT EXISTS idx_products_name ON products(name);
    CREATE INDEX IF NOT EXISTS idx_reviews_product_id ON reviews(product_id);
    CREATE INDEX IF NOT EXISTS idx_reviews_rating ON reviews(rating);
    CREATE INDEX IF NOT EXISTS idx_pm_product ON product_materials(product_id);
    CREATE INDEX IF NOT EXISTS idx_pm_material ON product_materials(material_id);
    """)
    con.commit()
    return con

def upsert_product(con, p):
    cur = con.cursor()
    cur.execute("""
    INSERT INTO products(source_domain,url,sku,name,brand,description,category,images,materials,created_at,updated_at)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    ON CONFLICT(url) DO UPDATE SET
      sku=excluded.sku, name=excluded.name, brand=excluded.brand,
      description=excluded.description, category=excluded.category,
      images=excluded.images, materials=excluded.materials,
      updated_at=excluded.updated_at
    """, (
        p["source_domain"], p["url"], p.get("sku"), p.get("name"), p.get("brand"),
        p.get("description"), p.get("category"),
        json.dumps(p.get("images") or [], ensure_ascii=False),
        json.dumps(p.get("materials") or {}, ensure_ascii=False),
        p["created_at"], p["updated_at"]
    ))
    con.commit()
    row = cur.execute("SELECT id FROM products WHERE url=?", (p["url"],)).fetchone()
    return row[0] if row else None

def insert_variant(con, product_id, v):
    cur = con.cursor()
    cur.execute("""
    INSERT INTO variants(product_id,variant_sku,color,size,upc,ean,gtin,price,currency,availability,raw)
    VALUES(?,?,?,?,?,?,?,?,?,?,?)
    """, (product_id, v.get("sku"), v.get("color"), v.get("size"),
          v.get("upc"), v.get("ean"), v.get("gtin"),
          v.get("price"), v.get("currency"), v.get("availability"),
          json.dumps(v, ensure_ascii=False)))
    con.commit()

def insert_review(con, product_id, r):
    unique_key = f"{product_id}|{r.get('author','')}|{r.get('published_at','')}|{(r.get('body') or '')[:120]}"
    unique_hash = hashlib.sha256(unique_key.encode("utf-8")).hexdigest()
    cur = con.cursor()
    try:
        cur.execute("""
        INSERT INTO reviews(product_id,rating,title,body,author,lang,published_at,source,raw,unique_hash)
        VALUES(?,?,?,?,?,?,?,?,?,?)
        """, (product_id, r.get("rating"), r.get("title"), r.get("body"),
              r.get("author"), r.get("lang"), r.get("published_at"),
              r.get("source"), json.dumps(r, ensure_ascii=False), unique_hash))
        con.commit()
    except sqlite3.IntegrityError:
        pass  # duplicate review

def upsert_material(con, name):
    name = normalize_material_name(name)
    if not name:
        return None
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO materials(name) VALUES (?)", (name,))
        con.commit()
        return cur.lastrowid
    except sqlite3.IntegrityError:
        row = cur.execute("SELECT id FROM materials WHERE name=?", (name,)).fetchone()
        return row[0] if row else None

def insert_product_material(con, product_id, material_id, percentage, source, raw):
    cur = con.cursor()
    try:
        cur.execute("""
        INSERT INTO product_materials(product_id, material_id, percentage, source, raw)
        VALUES(?,?,?,?,?)
        """, (product_id, material_id, percentage, source or '', raw or ''))
        con.commit()
    except sqlite3.IntegrityError:
        pass

# ---------- HTTP ----------
async def fetch(client, url):
    await asyncio.sleep(RATE_DELAY)
    r = await client.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r

# ---------- SITEMAPS ----------
async def discover_sitemaps(client):
    candidates = [urljoin(BASE_URL, p) for p in SITEMAP_HINTS]
    # robots.txt
    try:
        robots = await fetch(client, urljoin(BASE_URL, "robots.txt"))
        for line in robots.text.splitlines():
            if "Sitemap:" in line:
                sm = line.split("Sitemap:")[-1].strip()
                candidates.append(sm)
    except Exception:
        pass
    # validate xml-ish
    valid = []
    seen = set()
    for u in candidates:
        if u in seen:
            continue
        seen.add(u)
        try:
            r = await fetch(client, u)
            if r.status_code == 200 and ("<urlset" in r.text or "<sitemapindex" in r.text):
                valid.append(u)
        except Exception:
            continue
    return valid

def extract_xml_urls(xml_text):
    return [m.strip() for m in re.findall(r"<loc>(.*?)</loc>", xml_text)]

async def expand_all_sitemaps(client, sitemap_urls):
    urls = []
    for sm in sitemap_urls:
        try:
            r = await fetch(client, sm)
            txt = r.text
            locs = extract_xml_urls(txt)
            if "<sitemapindex" in txt:
                for sub in locs:
                    try:
                        rr = await fetch(client, sub)
                        urls.extend(extract_xml_urls(rr.text)[:MAX_URLS_PER_SITEMAP])
                    except Exception:
                        continue
            else:
                urls.extend(locs[:MAX_URLS_PER_SITEMAP])
        except Exception:
            continue
    # filter product-like URLs on the same domain
    product_like = []
    for u in urls:
        try:
            pr = urlparse(u)
            if pr.netloc.endswith(BASE_DOMAIN) and PRODUCT_PATH_RE.search(pr.path or ""):
                product_like.append(u)
        except Exception:
            continue
    # dedup keep order
    seen = set()
    out = []
    for u in product_like:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out

# ---------- PARSERS ----------
def parse_jsonld_product(soup):
    data = []
    for tag in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            txt = (tag.string or "").strip()
            if not txt:
                continue
            obj = json.loads(txt)
        except Exception:
            continue
        items = obj if isinstance(obj, list) else [obj]
        for it in items:
            if isinstance(it, dict) and it.get("@type") in ("Product", ["Product"]):
                data.append(it)
    return data

def safe_num(x):
    try:
        return float(x)
    except Exception:
        return None

def parse_schema_reviews(jd):
    reviews = []
    for it in jd:
        revs = it.get("review")
        if not revs:
            continue
        if isinstance(revs, dict):
            revs = [revs]
        for r in revs:
            reviews.append({
                "rating": safe_num(((r.get("reviewRating") or {}).get("ratingValue"))),
                "title": r.get("name"),
                "body": r.get("reviewBody"),
                "author": (r.get("author") or {}).get("name") if isinstance(r.get("author"), dict) else r.get("author"),
                "lang": None,
                "published_at": r.get("datePublished"),
                "source": "schema.org",
                "raw": r
            })
    return reviews

# ---------- FABRIC DETAILS / MATERIALS EXTRACTION ----------
def _norm_text(s):
    return re.sub(r"\s+", " ", (s or "").strip())

_FABRIC_HEAD_PATTERNS = [
    r"fabric details",
    r"materials?",
    r"material details",
    r"fabric",
    # Italian
    r"dettagli del tessuto",
    r"tessuto",
    r"materiali",
    # Spanish
    r"detalles del tejido",
    r"tejido",
    r"material(es)?",
    # French
    r"détails du tissu",
    r"tissu",
    r"mati(è|e)res?",
    # German
    r"material(ien)?",
]
_FABRIC_HEAD_RE = re.compile(r"^(" + r"|".join(_FABRIC_HEAD_PATTERNS) + r")$", re.I)

def parse_fabric_details_from_html(soup):
    section_text = []
    bullets = []
    found = None

    # Try headings first
    for h in soup.find_all(re.compile("h[1-6]")):
        txt = _norm_text(h.get_text(" "))
        if _FABRIC_HEAD_RE.match(txt) or re.search(r"(fabric|tessut|material|tissu|tejid)", txt, re.I):
            found = h
            break

    if not found:
        # Heuristic: look for strong/spans that look like section titles
        for tag in soup.find_all(["strong", "span", "div"], limit=1000):
            txt = _norm_text(tag.get_text(" "))
            if _FABRIC_HEAD_RE.match(txt):
                found = tag
                break

    if not found:
        return None

    # Collect content until the next heading
    for sib in found.next_siblings:
        name = getattr(sib, "name", None)
        if name and re.match(r"h[1-6]", name, re.I):
            break
        if name in ("section", "hr"):
            break
        if getattr(sib, "get_text", None):
            if name in ("ul", "ol"):
                for li in sib.find_all("li"):
                    bullets.append(_norm_text(li.get_text(" ")))
            else:
                txt = _norm_text(sib.get_text(" "))
                if txt:
                    section_text.append(txt)

    text_joined = _norm_text(" ".join(section_text)) if section_text else None
    bullets = [b for i, b in enumerate(bullets) if b and b not in bullets[:i]]
    if not text_joined and not bullets:
        return None
    return {"fabric_details_text": text_joined, "bullets": bullets}

def extract_materials_from_jsonld(jd):
    out = {}
    materials = []
    extra_props = {}
    for it in jd:
        mat = it.get("material")
        if isinstance(mat, list):
            materials.extend([_norm_text(x) for x in mat if _norm_text(x)])
        elif isinstance(mat, str):
            nm = _norm_text(mat)
            if nm:
                materials.append(nm)
        addp = it.get("additionalProperty") or it.get("additionalProperties")
        if addp:
            if isinstance(addp, dict):
                addp = [addp]
            for pv in addp:
                name = _norm_text(pv.get("name"))
                val = _norm_text(pv.get("value"))
                if name:
                    if re.search(r"(fabric|tessut|material|tissu|tejid|composition|shell|lining|pocket)", name, re.I) or val:
                        extra_props[name] = val or ""
    if materials:
        out["jsonld_material"] = list(dict.fromkeys(materials))
    if extra_props:
        out["extra_properties"] = extra_props
    return out or None

# --- Normalization helpers ---
_MAT_SYNONYMS = {
    "polyester": ["polyester", "poliestere"],
    "nylon": ["nylon", "polyamide", "polyamid", "poliammide", "polyamide 6.6", "polyamide 6,6", "pa"],
    "cotton": ["cotton", "cotone"],
    "elastane": ["elastane", "spandex", "elastan", "elastano", "lycra"],
    "wool": ["wool", "lana", "merino wool", "merino"],
    "down": ["down", "goose down", "duck down"],
    "hemp": ["hemp", "canapa"],
    "viscose": ["viscose", "rayon"],
    "modal": ["modal"],
    "polypropylene": ["polypropylene", "pp", "polipropilene"],
    "tencel": ["tencel", "lyocell"],
    "silk": ["silk", "seta"],
    "rubber": ["rubber", "gomma"],
    "eva": ["eva"],
    "gore-tex": ["gore-tex", "gore tex"],
}

def normalize_material_name(name):
    if not name:
        return None
    n = _norm_text(name).lower()
    n = re.sub(r"\b(recycled|riciclato|riciclata|post[- ]consumer|pre[- ]consumer|organic|biologico|responsible|certified|pfl|rds)\b", "", n)
    n = re.sub(r"[^a-z0-9 \-/\.]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    for canon, syns in _MAT_SYNONYMS.items():
        for s in syns:
            if re.search(rf"\b{s}\b", n):
                return canon
    return n.split("/")[0].split("-")[0].strip()

PCT_BEFORE = re.compile(r"(?P<pct>\d{1,3}(?:\.\d+)?)\s*%\s*(?P<mat>[A-Za-z][A-Za-z \-/\.]+)")
PCT_AFTER  = re.compile(r"(?P<mat>[A-Za-z][A-Za-z \-/\.]+?)\s*(?P<pct>\d{1,3}(?:\.\d+)?)\s*%")
BLEND_SPLIT = re.compile(r"[,/;]|(?:\s+\+\s+)")

def extract_material_compositions(text):
    out = []
    if not text:
        return out
    text = _norm_text(text)
    parts = [p.strip() for p in BLEND_SPLIT.split(text) if p.strip()]
    for part in parts:
        for m in PCT_BEFORE.finditer(part):
            mat = normalize_material_name(m.group("mat"))
            pct = float(m.group("pct"))
            if mat:
                out.append((mat, pct, part))
        for m in PCT_AFTER.finditer(part):
            mat = normalize_material_name(m.group("mat"))
            pct = float(m.group("pct"))
            if mat:
                out.append((mat, pct, part))
    if not out:
        candidates = re.findall(r"\b([A-Za-z][A-Za-z \-/\.]{2,})\b", text)
        for c in candidates:
            mat = normalize_material_name(c)
            if mat and len(mat) >= 3:
                out.append((mat, None, text))
    dedup = []
    seen = set()
    for mat, pct, raw in out:
        key = (mat, pct, raw)
        if key not in seen:
            dedup.append((mat, pct, raw))
            seen.add(key)
    return dedup

def collect_all_material_mentions(materials_payload):
    texts = []
    if not materials_payload:
        return texts
    if isinstance(materials_payload.get("fabric_details_text"), str):
        texts.append(materials_payload["fabric_details_text"])
    for b in materials_payload.get("bullets") or []:
        texts.append(b)
    for m in materials_payload.get("jsonld_material") or []:
        texts.append(m)
    extra = materials_payload.get("extra_properties") or {}
    for k, v in extra.items():
        if k:
            texts.append(k)
        if v:
            texts.append(v)
    return [t for t in texts if isinstance(t, str) and t.strip()]

# ---------- MAIN ----------
async def scrape():
    con = init_db()
    async with httpx.AsyncClient(http2=True, follow_redirects=True) as client:
        sm = await discover_sitemaps(client)
        if not sm:
            print("No sitemap found. Consider targeted category crawling.")
            return
        product_urls = await expand_all_sitemaps(client, sm)
        if LIMIT_FIRST_N_URLS:
            product_urls = product_urls[:LIMIT_FIRST_N_URLS]
        print(f"Found {len(product_urls)} candidate product URLs.")
        sem = asyncio.Semaphore(CONCURRENCY)

        async def handle(url):
            async with sem:
                try:
                    r = await fetch(client, url)
                except Exception as e:
                    print("Fetch error:", url, e)
                    return
                soup = BeautifulSoup(r.text, _PARSER)
                jd = parse_jsonld_product(soup)
                # coalesce product
                prod = None
                if jd:
                    p = jd[0]
                    offers = p.get("offers") or {}
                    if isinstance(offers, list) and offers:
                        offers = offers[0]
                    variants = []
                    if offers:
                        variants.append({
                            "sku": p.get("sku"),
                            "price": safe_num(offers.get("price")),
                            "currency": offers.get("priceCurrency"),
                            "availability": offers.get("availability"),
                        })
                    imgs = p.get("image")
                    images = imgs if isinstance(imgs, list) else ([imgs] if imgs else [])
                    brand = p.get("brand")
                    brand_name = (brand or {}).get("name") if isinstance(brand, dict) else brand
                    prod = {
                        "sku": p.get("sku"),
                        "name": p.get("name"),
                        "brand": brand_name,
                        "description": p.get("description"),
                        "category": p.get("category"),
                        "images": [i for i in images if i],
                        "variants": variants,
                        "raw": p
                    }
                if not prod:
                    return

                recs = parse_schema_reviews(jd)

                # --- Materials / Fabric Details ---
                materials_payload = {}
                html_fabric = parse_fabric_details_from_html(soup)
                if html_fabric:
                    materials_payload.update(html_fabric)
                jsonld_mats = extract_materials_from_jsonld(jd)
                if jsonld_mats:
                    materials_payload.update(jsonld_mats)

                # Save/Update product
                payload = {
                    "source_domain": BASE_DOMAIN,
                    "url": url,
                    "sku": prod.get("sku"),
                    "name": prod.get("name"),
                    "brand": prod.get("brand"),
                    "description": prod.get("description"),
                    "category": prod.get("category"),
                    "images": prod.get("images"),
                    "materials": materials_payload,
                    "created_at": now_iso(),
                    "updated_at": now_iso()
                }
                pid = upsert_product(con, payload)
                if not pid:
                    return

                # Insert variants
                for v in (prod.get("variants") or []):
                    insert_variant(con, pid, v)

                # Insert reviews
                for rr in recs:
                    insert_review(con, pid, rr)

                # --- Normalize materials into separate tables ---
                mentions = collect_all_material_mentions(materials_payload)
                html_texts = set()
                if materials_payload.get("fabric_details_text"):
                    html_texts.add(materials_payload["fabric_details_text"])
                html_texts.update(materials_payload.get("bullets") or [])
                jsonld_texts = set(materials_payload.get("jsonld_material") or [])

                for t in mentions:
                    comps = extract_material_compositions(t)
                    if not comps:
                        continue
                    for mat, pct, raw in comps:
                        mid = upsert_material(con, mat)
                        if not mid:
                            continue
                        source = "jsonld" if t in jsonld_texts else ("html" if t in html_texts else "extra")
                        insert_product_material(con, pid, mid, pct, source, raw)

        await asyncio.gather(*[handle(u) for u in product_urls])
    print("Done.")

if __name__ == "__main__":
    asyncio.run(scrape())
