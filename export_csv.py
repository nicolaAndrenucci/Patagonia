import sqlite3, csv, os

DB_PATH = "patagonia.db"
OUT_DIR = "exports_csv"

def export_table(con, table):
    os.makedirs(OUT_DIR, exist_ok=True)
    cur = con.cursor()
    rows = cur.execute(f"SELECT * FROM {table}").fetchall()
    cols = [d[0] for d in cur.description]
    out_path = os.path.join(OUT_DIR, f"{table}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print("Exported", table, "->", out_path)

if __name__ == "__main__":
    con = sqlite3.connect(DB_PATH)
    for t in ("products","variants","reviews"):
        export_table(con, t)
    print("Done.")
