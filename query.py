"""
13F Dataset Query Utilities
Analytical queries on the edgar_13f.db SQLite database.

Usage:
    python query.py --top-holdings --cik 0001067983 --quarter 2023-09-30
    python query.py --position-history --cusip 0231351067
    python query.py --consensus-buys --quarter 2023-09-30
    python query.py --summary
"""

import argparse
import sqlite3
import csv
import sys
from pathlib import Path
from typing import Optional

DB_PATH = Path("edgar_13f.db")


def connect(path: Path = DB_PATH) -> sqlite3.Connection:
    if not path.exists():
        print(f"ERROR: Database '{path}' not found. Run pipeline.py first.")
        sys.exit(1)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def print_table(rows, title: str = ""):
    if title:
        print(f"\n{'═'*70}")
        print(f"  {title}")
        print(f"{'═'*70}")
    if not rows:
        print("  (no results)")
        return
    keys = rows[0].keys()
    widths = {k: max(len(k), max(len(str(r[k] or "")) for r in rows)) for k in keys}
    header = "  " + "  ".join(k.ljust(widths[k]) for k in keys)
    print(header)
    print("  " + "  ".join("─" * widths[k] for k in keys))
    for row in rows:
        print("  " + "  ".join(str(row[k] or "").ljust(widths[k]) for k in keys))


def summary(conn: sqlite3.Connection):
    """High-level dataset statistics."""
    cur = conn.cursor()

    stats = {
        "Total filers":         cur.execute("SELECT COUNT(DISTINCT cik) FROM filers").fetchone()[0],
        "Total filings":        cur.execute("SELECT COUNT(*) FROM filers").fetchone()[0],
        "Total holdings rows":  cur.execute("SELECT COUNT(*) FROM holdings").fetchone()[0],
        "Unique CUSIPs":        cur.execute("SELECT COUNT(DISTINCT cusip) FROM holdings").fetchone()[0],
        "Earliest report date": cur.execute("SELECT MIN(report_date) FROM holdings").fetchone()[0],
        "Latest report date":   cur.execute("SELECT MAX(report_date) FROM holdings").fetchone()[0],
        "Total AUM ($M)":       cur.execute("SELECT ROUND(SUM(value_thousands)/1000.0,1) FROM holdings").fetchone()[0],
    }

    print(f"\n{'═'*50}")
    print("  13F Dataset Summary")
    print(f"{'═'*50}")
    for k, v in stats.items():
        print(f"  {k:<30} {v}")

    # Filings per year
    print(f"\n  {'─'*40}")
    print("  Filings per Year")
    print(f"  {'─'*40}")
    rows = cur.execute("""
        SELECT SUBSTR(report_date,1,4) AS year,
               COUNT(*) AS filings,
               COUNT(DISTINCT cik) AS managers
        FROM filers
        GROUP BY 1 ORDER BY 1
    """).fetchall()
    for r in rows:
        print(f"  {r['year']}  {r['filings']:>6} filings  {r['managers']:>4} managers")


def top_holdings(conn: sqlite3.Connection, cik: str, quarter: Optional[str] = None):
    """Top holdings by value for a given manager."""
    cik = cik.zfill(10)
    if not quarter:
        quarter = conn.execute(
            "SELECT MAX(report_date) FROM filers WHERE cik=?", (cik,)
        ).fetchone()[0]

    rows = conn.execute("""
        SELECT h.issuer_name, h.cusip, h.class_title,
               ROUND(h.value_thousands/1000.0, 2)   AS value_usd_m,
               h.shares_principal,
               h.investment_discretion,
               h.put_call
        FROM holdings h
        WHERE h.cik=? AND h.report_date=?
        ORDER BY h.value_thousands DESC
        LIMIT 25
    """, (cik, quarter)).fetchall()

    name = conn.execute(
        "SELECT manager_name FROM filers WHERE cik=? LIMIT 1", (cik,)
    ).fetchone()
    title = f"Top Holdings — {name[0] if name else cik}  [{quarter}]"
    print_table(rows, title)


def position_history(conn: sqlite3.Connection, cusip: str, cik: Optional[str] = None):
    """Quarter-over-quarter position history for a CUSIP."""
    sql = """
        SELECT f.manager_name, h.cik, h.report_date,
               ROUND(h.value_thousands/1000.0, 2) AS value_usd_m,
               h.shares_principal,
               h.put_call, h.investment_discretion
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.cusip=?
    """
    params = [cusip]
    if cik:
        sql += " AND h.cik=?"
        params.append(cik.zfill(10))
    sql += " ORDER BY h.cik, h.report_date"

    rows = conn.execute(sql, params).fetchall()
    print_table(rows, f"Position History — CUSIP {cusip}")


def consensus_buys(conn: sqlite3.Connection, quarter: str, min_managers: int = 5):
    """
    Securities newly added or increased by the most managers in a given quarter.
    Compares current quarter vs prior quarter.
    """
    rows = conn.execute("""
        WITH
        curr AS (
            SELECT cusip, issuer_name, cik,
                   SUM(shares_principal) AS shares,
                   SUM(value_thousands)  AS val
            FROM holdings
            WHERE report_date = ?
            GROUP BY cusip, issuer_name, cik
        ),
        prev AS (
            SELECT cusip, cik,
                   SUM(shares_principal) AS shares_prev
            FROM holdings
            WHERE report_date = (
                SELECT MAX(report_date) FROM holdings
                WHERE report_date < ?
            )
            GROUP BY cusip, cik
        ),
        changes AS (
            SELECT c.cusip, c.issuer_name, c.cik,
                   c.shares, c.val,
                   COALESCE(p.shares_prev, 0) AS shares_prev,
                   CASE
                     WHEN p.cik IS NULL           THEN 'NEW'
                     WHEN c.shares > p.shares_prev THEN 'ADD'
                     WHEN c.shares < p.shares_prev THEN 'CUT'
                     ELSE 'HOLD'
                   END AS action
            FROM curr c
            LEFT JOIN prev p ON p.cusip=c.cusip AND p.cik=c.cik
        )
        SELECT cusip, issuer_name,
               COUNT(*) AS num_managers,
               SUM(CASE WHEN action='NEW' THEN 1 ELSE 0 END) AS new_positions,
               SUM(CASE WHEN action='ADD' THEN 1 ELSE 0 END) AS add_positions,
               ROUND(SUM(val)/1000.0, 1) AS total_value_usd_m
        FROM changes
        WHERE action IN ('NEW','ADD')
        GROUP BY cusip, issuer_name
        HAVING num_managers >= ?
        ORDER BY num_managers DESC, total_value_usd_m DESC
        LIMIT 30
    """, (quarter, quarter, min_managers)).fetchall()

    print_table(rows, f"Consensus Buys — {quarter}  (≥{min_managers} managers)")


def manager_panel(conn: sqlite3.Connection, cik: str, out_csv: Optional[str] = None):
    """Full manager–security–quarter panel for one filer."""
    cik = cik.zfill(10)
    rows = conn.execute("""
        SELECT f.manager_name, h.cik, h.report_date, f.filing_date,
               h.issuer_name, h.cusip, h.class_title,
               h.value_thousands,
               h.shares_principal, h.shares_type,
               h.put_call, h.investment_discretion,
               h.voting_sole, h.voting_shared, h.voting_none
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.cik=?
        ORDER BY h.report_date DESC, h.value_thousands DESC
    """, (cik,)).fetchall()

    if out_csv:
        with open(out_csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(rows[0].keys() if rows else [])
            w.writerows(rows)
        print(f"Exported {len(rows):,} rows → {out_csv}")
    else:
        print_table(rows[:50], f"Manager Panel — CIK {cik}  (showing first 50 rows)")
        if len(rows) > 50:
            print(f"\n  … {len(rows)-50:,} more rows. Use --export-csv to save full panel.")


def overlap_matrix(conn: sqlite3.Connection, quarter: str, top_n: int = 10):
    """Portfolio overlap % between top managers for a given quarter."""
    managers = conn.execute("""
        SELECT h.cik, f.manager_name,
               COUNT(DISTINCT h.cusip) AS n_holdings
        FROM holdings h
        JOIN filers f ON f.cik=h.cik AND f.report_date=h.report_date
        WHERE h.report_date=?
        GROUP BY h.cik
        ORDER BY n_holdings DESC
        LIMIT ?
    """, (quarter, top_n)).fetchall()

    print(f"\n{'═'*60}")
    print(f"  Portfolio Overlap Matrix — {quarter}")
    print(f"{'═'*60}")
    ciks = [(r["cik"], r["manager_name"][:18]) for r in managers]

    # header
    print("  " + " " * 20 + "".join(f"{n:>8}" for _, n in ciks))
    for cik_a, name_a in ciks:
        row_str = f"  {name_a:<20}"
        for cik_b, _ in ciks:
            if cik_a == cik_b:
                row_str += "    100%"
            else:
                result = conn.execute("""
                    SELECT COUNT(*) FROM (
                        SELECT cusip FROM holdings
                        WHERE cik=? AND report_date=?
                        INTERSECT
                        SELECT cusip FROM holdings
                        WHERE cik=? AND report_date=?
                    )
                """, (cik_a, quarter, cik_b, quarter)).fetchone()[0]
                total_a = conn.execute(
                    "SELECT COUNT(DISTINCT cusip) FROM holdings WHERE cik=? AND report_date=?",
                    (cik_a, quarter)
                ).fetchone()[0]
                pct = round(result / total_a * 100) if total_a else 0
                row_str += f"    {pct:>3}%"
        print(row_str)


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Query the 13F EDGAR dataset")
    parser.add_argument("--db", default="edgar_13f.db")

    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("summary")

    p = sub.add_parser("top-holdings")
    p.add_argument("--cik", required=True)
    p.add_argument("--quarter")

    p = sub.add_parser("position-history")
    p.add_argument("--cusip", required=True)
    p.add_argument("--cik")

    p = sub.add_parser("consensus-buys")
    p.add_argument("--quarter", required=True)
    p.add_argument("--min-managers", type=int, default=5)

    p = sub.add_parser("manager-panel")
    p.add_argument("--cik", required=True)
    p.add_argument("--export-csv")

    p = sub.add_parser("overlap")
    p.add_argument("--quarter", required=True)
    p.add_argument("--top-n", type=int, default=10)

    args = parser.parse_args()
    conn = connect(Path(args.db))

    if args.cmd == "summary":
        summary(conn)
    elif args.cmd == "top-holdings":
        top_holdings(conn, args.cik, args.quarter)
    elif args.cmd == "position-history":
        position_history(conn, args.cusip, args.cik)
    elif args.cmd == "consensus-buys":
        consensus_buys(conn, args.quarter, args.min_managers)
    elif args.cmd == "manager-panel":
        manager_panel(conn, args.cik, args.export_csv)
    elif args.cmd == "overlap":
        overlap_matrix(conn, args.quarter, args.top_n)
    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()
