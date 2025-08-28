# app.py
import os, re, sqlite3
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Imposta il percorso del DB (read-only via URI). Es.: SQLITE_URI="file:./data/prodotti.db?mode=ro&cache=shared"
SQLITE_URI = os.environ.get("SQLITE_URI", "file:./db.sqlite?mode=ro&cache=shared")

app = FastAPI(title="SQLite Tool API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Blocca tutto ciò che non sia SELECT/CTE
BLOCK = re.compile(r"\b(INSERT|UPDATE|DELETE|REPLACE|CREATE|ALTER|DROP|ATTACH|DETACH|VACUUM|PRAGMA|TRUNCATE)\b", re.I)

def query(sql: str, params: dict, limit: int = 200):
    if not sql.strip():
        raise HTTPException(400, "SQL mancante")
    # Consenti SELECT e WITH; vieta DDL/DML/PRAGMA
    if BLOCK.search(sql):
        raise HTTPException(400, "Solo query di lettura (SELECT/WITH) sono permesse")
    # Applica un LIMIT di sicurezza se non presente
    if re.search(r"\blimit\b", sql, re.I) is None:
        sql = f"{sql.rstrip()}\nLIMIT {limit}"
    # Connessione per richiesta, in sola lettura
    conn = sqlite3.connect(SQLITE_URI, uri=True, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute(sql, params or {})
        rows = [dict(r) for r in cur.fetchall()]
        return {"rows": rows, "row_count": len(rows)}
    finally:
        conn.close()

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/tools/run_sql")
def run_sql(payload: dict):
    sql = payload.get("sql")
    params = payload.get("params", {})
    limit = int(payload.get("limit", 200))
    return query(sql, params, limit)
