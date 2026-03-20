"""
Phase 2 – Security Classification
==================================
Classifies every unique security in edgar_13f.db using:
  1. put_call field       (confidence 1.00) — options always explicit
  2. SEC 13F official list (confidence 0.97) — fetched when available
  3. class_title rules    (confidence 0.78–0.97) — pattern matching
  4. issuer_name rules    (confidence 0.75–0.92) — known ETF providers, ADR markers
  5. shares_type = PRN    (confidence 0.72) — principal amount → bond
  6. Default              (confidence 0.50) → other

Output categories
-----------------
  common_stock | etf | option_call | option_put | adr | preferred
  bond | warrant | right | unit | fund | partnership | other

Deliverable: security_master table with classification + confidence_score.

Usage:
    python phase2.py                         # classify / re-classify all
    python phase2.py --stats                 # print summary after run
    python phase2.py --export-csv            # save security_master.csv
    python phase2.py --unclassified          # show 'other' rows for review
    python phase2.py --override CUSIP CLASS  # manually set one security
"""

import argparse
import csv
import json
import re
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Optional

DB_PATH = Path("edgar_13f.db")
HEADERS = {"User-Agent": "13F-SEC-Phase2 research@example.com"}

# ── Valid output classes ────────────────────────────────────────────────────────
VALID_CLASSES = {
    "common_stock", "etf", "option_call", "option_put",
    "adr", "preferred", "bond", "warrant", "right",
    "unit", "fund", "partnership", "other",
}

# ── Rule 1: put_call field ──────────────────────────────────────────────────────
PUT_CALL_MAP = {
    "put":  ("option_put",  1.00, "put_call_field"),
    "call": ("option_call", 1.00, "put_call_field"),
}

# ── Rule 3: class_title rules (first match wins, applied to UPPER-STRIPPED title)
# Each tuple: (compiled_regex, classification, confidence, source_tag)
_CLASS_RULES_RAW = [
    # Options (explicit in class_title, though usually in put_call field)
    (r"\bCALL\b",                                           "option_call",  0.97, "ct_call"),
    (r"\bPUT\b",                                            "option_put",   0.97, "ct_put"),
    # ETF / Exchange-Traded Products
    (r"\bETF\b|\bETP\b|EXCHANGE.TRADED",                   "etf",          0.97, "ct_etf"),
    # ADR / ADS / Depositary Receipts
    (r"\bADR\b|A\.D\.R\.|\bADS\b|\bGDR\b|"
     r"AMER\.?\s*(DEP|DR)\b|AMERICAN\s+DEP|"
     r"SPON(sor)?[ED]?\s*(ADR|ADS|ADS?)|"
     r"NY\s+REGISTRY|N\.?Y\.?\s+REGISTRY",                 "adr",          0.95, "ct_adr"),
    # Preferred stock
    (r"\bPRF\b|\bPFD\b|\bPREF(ERRED)?\b",                 "preferred",    0.93, "ct_preferred"),
    # Bonds / Notes / Debt
    (r"\bNOT[ES]{0,2}\b|\bBOND\b|\bDBT\b|"
     r"DEBEN|SENIOR\s+NOTE|SUB(ORDINATE)?\.?\s*NOTE|"
     r"SR\.?\s*NOTE|CONV.*NOTE|\bDEBT\b|"
     r"\d+\.?\d*\s*%|\bDUE\s+\d{4}\b",                    "bond",         0.92, "ct_bond"),
    # Warrants
    (r"WARRANT|\bWTS?\b",                                  "warrant",      0.92, "ct_warrant"),
    # Rights
    (r"\bRIGHT[S]?\b|\bRTS\b",                            "right",        0.90, "ct_right"),
    # REITs / SH BEN INT / BEN INT (beneficial interest)
    (r"SH\s+BEN\s+INT|SHS?\s+BEN\s+IN|"
     r"UNIT\s+BEN\s+INT|BEN\s+INT",                        "fund",         0.80, "ct_benint"),
    # Partnerships / LP units
    (r"\bLP\b|\bMLP\b|LTD\s+PARTN|UNIT\s+LTD\s+PARTN|"
     r"COM\s+UNIT\s+REP\s+LTD|UNIT\s+REP\s+LTD",         "partnership",  0.88, "ct_partnership"),
    # Trust / Fund
    (r"\bTR\s+UNIT\b|\bTRUST\s+UNIT\b",                   "fund",         0.82, "ct_trust_unit"),
    (r"\bFUND\b",                                          "fund",         0.78, "ct_fund"),
    # Common stock  (broad catch-all — after more specific ones above)
    (r"\bCOM\b|COMMON|ORDINARY|\bORD\b|"
     r"CAP\s+STK|\bSTK\b|COM\s+STK|"
     r"CL\.?\s+[A-Z]\b|CLASS\s+[A-Z]\b|"
     r"\bNPV\b|NON\s+VTG|SUB\s+VTG",                      "common_stock", 0.90, "ct_common"),
    # Shares (weaker — many types use SHS/SH)
    (r"\bSHS?\b|\bSHARES?\b",                              "common_stock", 0.72, "ct_shares"),
    # German/foreign ordinary shares fallback
    (r"NAMEN\s+AKT|INHABER|BEARER",                        "common_stock", 0.80, "ct_foreign_ord"),
    # Warrants with expiry date pattern (e.g. "*W EXP 08/03/2025")
    (r"\*?W\s+EXP\s+\d",                                   "warrant",      0.93, "ct_warrant_exp"),
    # Trust units with date placeholder (e.g. "UNIT 99/99/9999")
    (r"UNIT\s+\d{2}/\d{2}/\d{4}",                          "fund",         0.80, "ct_unit_date"),
    # Partnership unit explicit
    (r"PARTNER(SHIP)?\s+UNIT",                              "partnership",  0.90, "ct_partnership_unit"),
    # American registry / cert (foreign registrar shares)
    (r"AMER\s+REG|AMER\s+CERT|CERT\s+DEP",                 "adr",          0.82, "ct_amer_reg"),
]
CLASS_TITLE_RULES = [
    (re.compile(p, re.IGNORECASE), cls, conf, src)
    for p, cls, conf, src in _CLASS_RULES_RAW
]

# ── Rule 4: issuer_name rules ────────────────────────────────────────────────────
_NAME_RULES_RAW = [
    # Known ETF brand names (high precision)
    (r"\biSHARES\b|ISHARES",                                        "etf",         0.92, "name_ishares"),
    (r"\bSPDR\b",                                                   "etf",         0.92, "name_spdr"),
    (r"PROSHARES|PRO\s+SHARES",                                     "etf",         0.90, "name_proshares"),
    (r"POWERSHARES|POWER\s+SHARES",                                 "etf",         0.90, "name_powershares"),
    (r"DIREXION\s+(DAILY|SHARES|ETF|BULL|BEAR)",                    "etf",         0.90, "name_direxion"),
    (r"INVESCO\s+(QQQ|ETF|TRUST|S&P|NASDAQ)",                       "etf",         0.88, "name_invesco_etf"),
    (r"WISDOMTREE|WISDOM\s+TREE",                                   "etf",         0.90, "name_wisdomtree"),
    (r"VANECK\s+(ETF|VECTORS?|MERK)|VAN\s+ECK",                     "etf",         0.88, "name_vaneck"),
    (r"FIRST\s+TRUST\s+(ETF|EXC|NASD)",                             "etf",         0.88, "name_firsttrust"),
    (r"GLOBAL\s+X\s+(ETF|FUND|MSCI)",                               "etf",         0.88, "name_globalx"),
    (r"AMPLIFY\s+ETF|PACER\s+ETF|ROUNDHILL\s+ETF",                  "etf",         0.88, "name_misc_etf"),
    (r"SCHWAB\s+(US|INTL|ETF|BROAD)|CHARLES\s+SCHWAB.*ETF",        "etf",         0.88, "name_schwab_etf"),
    (r"VANGUARD\s+(S&P|TOTAL|FTSE|REIT|DIVIDEND|ETF|GROWTH|VALUE)",  "etf",        0.88, "name_vanguard_etf"),
    (r"FIDELITY\s+(MSCI|ETF|MOMENTUM|VALUE|QUAL)",                  "etf",         0.85, "name_fidelity_etf"),
    (r"SSGA\s+FUNDS|STATE\s+STREET.*ETF",                           "etf",         0.85, "name_ssga_etf"),
    (r"XTRACKERS|DEUTSCHE\s+.*ETF",                                 "etf",         0.85, "name_xtrackers"),
    (r"JPMORGAN\s+.*ETF|JP\s+MORGAN\s+.*ETF",                       "etf",         0.85, "name_jpm_etf"),
    (r"\bETF\b",                                                    "etf",         0.82, "name_etf_generic"),
    # Vanguard fund families (ETFs and index funds)
    (r"VANGUARD\s+(BD\b|BOND|MUN\b|WHITEHALL|SCOTTSDALE|WORLD\s+FD|INTL\s+EQ)",
                                                                "etf",         0.88, "name_vanguard_fund"),
    (r"VANGUARD\s+(TAX.MANAGED|FTSE|ALLWRLD|MCAP|RUS\d|TOTAL\s+BND|HIGH\s+DIV|DIV\s+YLD)",
                                                                "etf",         0.88, "name_vanguard_idx"),
    # Invesco exchange-traded
    (r"INVESCO\s+(EXCH|EXCHANGE|ACTIVELY|ACTVELY)",              "etf",         0.88, "name_invesco_exch"),
    # KraneShares
    (r"KRANESHARES",                                             "etf",         0.88, "name_kraneshares"),
    # ADR indicators in issuer name
    (r"\bADR\b|AMER\.?\s+DEP\b|AMERICAN\s+DEPOSITARY|"
     r"\bADS\b(?!\w)|\bGDR\b",                                      "adr",         0.85, "name_adr"),
    # Preferred
    (r"PREFERRED|PREF(\.|\s|$)",                                    "preferred",   0.82, "name_preferred"),
    # Warrants
    (r"WARRANT",                                                    "warrant",     0.88, "name_warrant"),
    # Notes / bonds
    (r"\d+\.?\d+\s*%.*NOTE[S]?|NOTES?\s+DUE\s+\d{4}|"
     r"SENIOR\s+NOTE|SUB(ORDINATE)?\s+NOTE",                        "bond",        0.82, "name_bond"),
    # Partnerships / MLPs
    (r"\bL\.?P\.?\b(?!\s+MORGAN)|\bMLP\b|LIMITED\s+PARTNERSHIP|PARTNERS,?\s+L\.?P\.?",
                                                                "partnership", 0.82, "name_partnership"),
    # Warrants with expiry in name
    (r"\*?W\s+EXP|WARRANT\s+EXP",                              "warrant",     0.92, "name_warrant_exp"),
]
ISSUER_NAME_RULES = [
    (re.compile(p, re.IGNORECASE), cls, conf, src)
    for p, cls, conf, src in _NAME_RULES_RAW
]

# ── DB schema ────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS security_master (
    cusip                TEXT PRIMARY KEY,
    issuer_name          TEXT,
    class_title_raw      TEXT,
    classification       TEXT NOT NULL,
    confidence_score     REAL NOT NULL,
    classification_source TEXT,
    manual_override      INTEGER DEFAULT 0,
    first_seen_quarter   TEXT,
    last_seen_quarter    TEXT,
    times_held           INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sm_class  ON security_master(classification);
CREATE INDEX IF NOT EXISTS idx_sm_conf   ON security_master(confidence_score);
"""


# ── Core classifier ─────────────────────────────────────────────────────────────

def classify(
    cusip: str,
    issuer_name: Optional[str],
    class_title: Optional[str],
    put_call: Optional[str],
    shares_type: Optional[str],
    sec_list: dict,
) -> tuple[str, float, str]:
    """
    Returns (classification, confidence_score, source_tag).
    Priority: put_call  >  SEC official list  >  class_title rules
              >  issuer_name rules  >  shares_type=PRN  >  default
    """

    # 1. put_call field — highest confidence
    if put_call:
        result = PUT_CALL_MAP.get(put_call.strip().lower())
        if result:
            return result

    # 2. SEC 13F official list lookup by CUSIP
    if cusip in sec_list:
        return (sec_list[cusip], 0.97, "sec_13f_list")

    # 3. class_title rules
    ct = (class_title or "").strip()
    if ct:
        for pattern, cls, conf, src in CLASS_TITLE_RULES:
            if pattern.search(ct):
                return (cls, conf, src)

    # 4. issuer_name rules
    name = (issuer_name or "").strip()
    if name:
        for pattern, cls, conf, src in ISSUER_NAME_RULES:
            if pattern.search(name):
                return (cls, conf, src)

    # 5. shares_type = PRN (principal amount → bond)
    if shares_type and shares_type.strip().upper() == "PRN":
        return ("bond", 0.72, "shares_type_prn")

    # 6. Default
    return ("other", 0.50, "default")


# ── SEC 13F official list (best-effort, stdlib only) ───────────────────────────

def _fetch(url: str, timeout: int = 15) -> bytes:
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read()
    except Exception:
        return b""


def fetch_sec_13f_list(quarter: Optional[str] = None) -> dict:
    """
    Attempts to download the SEC official 13F reporting list for the given
    quarter (YYYY-MM-DD format, on a quarter-end date).
    Returns dict {cusip: classification} — empty on failure.

    The SEC publishes the list as a PDF. We first try a known plain-text
    companion URL pattern; if unavailable we return an empty dict and fall
    back to rule-based classification.
    """
    # Derive year/quarter number
    if not quarter:
        from datetime import date
        today = date.today()
        month = today.month
        year  = today.year
        qnum  = (month - 1) // 3 + 1
    else:
        y, m, _ = quarter.split("-")
        year  = int(y)
        qnum  = (int(m) - 1) // 3 + 1

    # Try HTML version of the SEC 13F list page to find text/csv links
    list_page = _fetch(
        "https://www.sec.gov/divisions/investment/13flists.htm", timeout=10
    )
    if not list_page:
        return {}

    # Look for any downloadable list file matching our quarter
    text = list_page.decode("utf-8", errors="ignore")
    # Pattern e.g. "13flist2025q4.pdf" or "13flist2025q4.htm"
    candidates = re.findall(
        rf"13flist{year}q{qnum}(?:amended)?\.(?:txt|htm|csv|pdf)",
        text, re.IGNORECASE,
    )
    if not candidates:
        return {}

    base = "https://www.sec.gov/divisions/investment/13f/"
    result: dict = {}
    for fname in candidates:
        if fname.lower().endswith(".pdf"):
            continue  # can't parse PDF with stdlib
        raw = _fetch(base + fname, timeout=20)
        if not raw:
            continue
        content = raw.decode("utf-8", errors="ignore")
        # Parse SEC 13F list lines: CUSIP is in first 9 chars of each relevant line
        # Format varies by year but CUSIP is reliably 9 chars in first column
        for line in content.splitlines():
            line = line.strip()
            if len(line) < 9:
                continue
            # Skip header lines
            if re.match(r"[A-Z\s]{5,}", line[:9]):
                continue
            cusip_cand = line[:9].replace(" ", "")
            if len(cusip_cand) == 9 and re.match(r"[A-Z0-9]{9}", cusip_cand):
                # Try to infer type from rest of line
                rest = line[9:].upper()
                if "EQUITY" in rest or "COM" in rest:
                    result[cusip_cand] = "common_stock"
                elif "OPTION" in rest:
                    result[cusip_cand] = "option_call"  # generic; put_call field overrides
                elif "ETF" in rest or "EXCHANGE" in rest:
                    result[cusip_cand] = "etf"
                elif "ADR" in rest or "DEPOSITARY" in rest:
                    result[cusip_cand] = "adr"
                elif "PREFERRED" in rest or "PRF" in rest:
                    result[cusip_cand] = "preferred"
                elif "NOTE" in rest or "BOND" in rest or "DEBT" in rest:
                    result[cusip_cand] = "bond"
        if result:
            print(f"  Loaded {len(result):,} entries from SEC 13F list ({fname})")
            break

    return result


# ── DB helpers ──────────────────────────────────────────────────────────────────

def init_db(conn: sqlite3.Connection) -> None:
    with conn:
        conn.executescript(SCHEMA)


def load_securities(conn: sqlite3.Connection) -> list[dict]:
    """
    Load every unique CUSIP with its most-common class_title and issuer_name,
    plus put_call / shares_type signals, quarter range, and hold count.
    """
    rows = conn.execute("""
        SELECT
            cusip,
            -- most-frequent issuer name for this CUSIP
            (SELECT issuer_name FROM holdings h2 WHERE h2.cusip=h.cusip
             GROUP BY issuer_name ORDER BY COUNT(*) DESC LIMIT 1)  AS issuer_name,
            -- most-frequent class_title
            (SELECT class_title FROM holdings h3 WHERE h3.cusip=h.cusip
             GROUP BY class_title ORDER BY COUNT(*) DESC LIMIT 1)  AS class_title,
            -- any non-null put_call value
            (SELECT put_call FROM holdings h4
             WHERE h4.cusip=h.cusip AND put_call IS NOT NULL
             LIMIT 1)                                              AS put_call,
            -- any non-null shares_type
            (SELECT shares_type FROM holdings h5
             WHERE h5.cusip=h.cusip AND shares_type IS NOT NULL
             LIMIT 1)                                              AS shares_type,
            MIN(report_date)   AS first_seen,
            MAX(report_date)   AS last_seen,
            COUNT(*)           AS times_held
        FROM holdings h
        GROUP BY cusip
    """).fetchall()
    cols = [d[0] for d in conn.execute("SELECT * FROM holdings LIMIT 0").description]
    # custom cols from above query
    return [
        {
            "cusip":        r[0],
            "issuer_name":  r[1],
            "class_title":  r[2],
            "put_call":     r[3],
            "shares_type":  r[4],
            "first_seen":   r[5],
            "last_seen":    r[6],
            "times_held":   r[7],
        }
        for r in rows
    ]


def get_overridden_cusips(conn: sqlite3.Connection) -> set:
    try:
        rows = conn.execute(
            "SELECT cusip FROM security_master WHERE manual_override=1"
        ).fetchall()
        return {r[0] for r in rows}
    except Exception:
        return set()


def upsert_security_master(
    conn: sqlite3.Connection,
    rows: list[dict],
    skip_cusips: set,
) -> None:
    with conn:
        conn.executemany("""
            INSERT INTO security_master
                (cusip, issuer_name, class_title_raw, classification,
                 confidence_score, classification_source, manual_override,
                 first_seen_quarter, last_seen_quarter, times_held)
            VALUES
                (:cusip, :issuer_name, :class_title_raw, :classification,
                 :confidence_score, :classification_source, 0,
                 :first_seen, :last_seen, :times_held)
            ON CONFLICT(cusip) DO UPDATE SET
                issuer_name          = excluded.issuer_name,
                class_title_raw      = excluded.class_title_raw,
                classification       = CASE WHEN manual_override=1
                                            THEN classification
                                            ELSE excluded.classification END,
                confidence_score     = CASE WHEN manual_override=1
                                            THEN confidence_score
                                            ELSE excluded.confidence_score END,
                classification_source= CASE WHEN manual_override=1
                                            THEN classification_source
                                            ELSE excluded.classification_source END,
                first_seen_quarter   = excluded.first_seen_quarter,
                last_seen_quarter    = excluded.last_seen_quarter,
                times_held           = excluded.times_held
            WHERE manual_override = 0   -- never overwrite manual overrides automatically
        """, [r for r in rows if r["cusip"] not in skip_cusips])


# ── Main classification run ─────────────────────────────────────────────────────

def run_classification(conn: sqlite3.Connection, verbose: bool = False) -> None:
    print("Loading securities from holdings …")
    securities = load_securities(conn)
    print(f"  {len(securities):,} unique CUSIPs found.")

    print("Fetching SEC 13F official list (best-effort) …")
    sec_list = {}
    try:
        sec_list = fetch_sec_13f_list()
    except Exception as e:
        print(f"  Warning: SEC list fetch failed ({e}). Rule-based only.")
    if not sec_list:
        print("  No SEC list loaded — using rule-based classification only.")

    overridden = get_overridden_cusips(conn)
    if overridden:
        print(f"  {len(overridden)} manually-overridden CUSIPs will be preserved.")

    print("Classifying …")
    rows_to_insert = []
    for sec in securities:
        cls, score, src = classify(
            cusip       = sec["cusip"],
            issuer_name = sec["issuer_name"],
            class_title = sec["class_title"],
            put_call    = sec["put_call"],
            shares_type = sec["shares_type"],
            sec_list    = sec_list,
        )
        rows_to_insert.append({
            "cusip":                 sec["cusip"],
            "issuer_name":           sec["issuer_name"],
            "class_title_raw":       sec["class_title"],
            "classification":        cls,
            "confidence_score":      round(score, 4),
            "classification_source": src,
            "first_seen":            sec["first_seen"],
            "last_seen":             sec["last_seen"],
            "times_held":            sec["times_held"],
        })

    upsert_security_master(conn, rows_to_insert, overridden)
    print(f"  Upserted {len(rows_to_insert):,} rows into security_master.")


# ── Stats ────────────────────────────────────────────────────────────────────────

def print_stats(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM security_master").fetchone()[0]
    print(f"\n{'═'*60}")
    print(f"  Security Master — Classification Summary")
    print(f"{'═'*60}")
    print(f"  Total unique securities: {total:,}")
    print()

    rows = conn.execute("""
        SELECT classification,
               COUNT(*)                              AS n_securities,
               SUM(times_held)                      AS total_holdings,
               ROUND(AVG(confidence_score)*100, 1)  AS avg_conf_pct,
               ROUND(MIN(confidence_score)*100, 1)  AS min_conf_pct
        FROM security_master
        GROUP BY classification
        ORDER BY n_securities DESC
    """).fetchall()

    print(f"  {'Classification':<18} {'# Securities':>13} {'# Holdings':>11} "
          f"{'Avg Conf%':>10} {'Min Conf%':>10}")
    print(f"  {'─'*18} {'─'*13} {'─'*11} {'─'*10} {'─'*10}")
    for r in rows:
        print(f"  {r[0]:<18} {r[1]:>13,} {r[2]:>11,} {r[3]:>9.1f}% {r[4]:>9.1f}%")

    print()
    # Confidence distribution
    conf_dist = conn.execute("""
        SELECT
            CASE
                WHEN confidence_score >= 0.95 THEN 'HIGH   (≥0.95)'
                WHEN confidence_score >= 0.80 THEN 'MEDIUM (0.80–0.94)'
                WHEN confidence_score >= 0.65 THEN 'LOW    (0.65–0.79)'
                ELSE                               'WEAK   (<0.65)'
            END AS band,
            COUNT(*) AS n
        FROM security_master
        GROUP BY 1 ORDER BY MIN(confidence_score) DESC
    """).fetchall()
    print(f"  Confidence Distribution")
    print(f"  {'─'*40}")
    for r in conf_dist:
        bar = "█" * (r[1] * 30 // max(x[1] for x in conf_dist))
        print(f"  {r[0]:<22}  {r[1]:>6,}  {bar}")

    # Source breakdown
    print()
    src_rows = conn.execute("""
        SELECT classification_source, COUNT(*) AS n
        FROM security_master GROUP BY 1 ORDER BY 2 DESC
    """).fetchall()
    print(f"  Classification Source Breakdown")
    print(f"  {'─'*40}")
    for r in src_rows:
        print(f"  {r[0]:<35}  {r[1]:>6,}")


# ── CSV export ───────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection, path: str = "security_master.csv") -> None:
    rows = conn.execute("""
        SELECT cusip, issuer_name, class_title_raw, classification,
               confidence_score, classification_source, manual_override,
               first_seen_quarter, last_seen_quarter, times_held
        FROM security_master
        ORDER BY classification, confidence_score DESC
    """).fetchall()
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["cusip","issuer_name","class_title_raw","classification",
                    "confidence_score","classification_source","manual_override",
                    "first_seen_quarter","last_seen_quarter","times_held"])
        w.writerows(rows)
    print(f"Exported {len(rows):,} rows → {path}")


# ── Show unclassified ────────────────────────────────────────────────────────────

def show_unclassified(conn: sqlite3.Connection, limit: int = 50) -> None:
    rows = conn.execute("""
        SELECT cusip, issuer_name, class_title_raw,
               confidence_score, times_held
        FROM security_master
        WHERE classification='other'
        ORDER BY times_held DESC
        LIMIT ?
    """, (limit,)).fetchall()
    print(f"\n{'═'*70}")
    print(f"  'other' classifications (top {limit} by frequency)")
    print(f"{'═'*70}")
    print(f"  {'CUSIP':<12} {'Issuer':<35} {'ClassTitle':<20} {'Conf':>5} {'Times':>6}")
    print(f"  {'─'*12} {'─'*35} {'─'*20} {'─'*5} {'─'*6}")
    for r in rows:
        print(f"  {r[0]:<12} {str(r[1] or '')[:35]:<35} "
              f"{str(r[2] or '')[:20]:<20} {r[3]:>5.2f} {r[4]:>6}")


# ── Manual override ──────────────────────────────────────────────────────────────

def apply_override(conn: sqlite3.Connection, cusip: str, classification: str) -> None:
    if classification not in VALID_CLASSES:
        print(f"ERROR: '{classification}' is not a valid class. Choose from:")
        print("  " + "  |  ".join(sorted(VALID_CLASSES)))
        sys.exit(1)
    with conn:
        conn.execute("""
            UPDATE security_master
            SET classification=?, confidence_score=1.0,
                classification_source='manual_override', manual_override=1
            WHERE cusip=?
        """, (classification, cusip))
        if conn.execute("SELECT changes()").fetchone()[0] == 0:
            print(f"CUSIP {cusip} not found in security_master. Run phase2.py first.")
        else:
            print(f"Override applied: {cusip} → {classification}")


# ── CLI ──────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Phase 2 – Security classification for SEC 13F holdings"
    )
    parser.add_argument("--db",          default=str(DB_PATH),
                        help="Path to edgar_13f.db (default: edgar_13f.db)")
    parser.add_argument("--stats",       action="store_true",
                        help="Print classification summary")
    parser.add_argument("--export-csv",  metavar="FILE", nargs="?", const="security_master.csv",
                        help="Export security_master to CSV")
    parser.add_argument("--unclassified",action="store_true",
                        help="Show securities classified as 'other'")
    parser.add_argument("--override",    nargs=2, metavar=("CUSIP", "CLASS"),
                        help="Manually override a CUSIP's classification")
    parser.add_argument("--no-fetch",    action="store_true",
                        help="Skip SEC 13F list fetch (faster, offline)")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"ERROR: Database not found: {db_path}\nRun pipeline.py first.")
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    init_db(conn)

    if args.override:
        apply_override(conn, args.override[0], args.override[1])
    elif args.unclassified:
        show_unclassified(conn)
    elif args.export_csv:
        export_csv(conn, args.export_csv)
    else:
        # Default: run classification, then always show stats
        run_classification(conn, verbose=False)
        print_stats(conn)

    if args.stats and not (args.override or args.unclassified or args.export_csv):
        pass  # already printed above
    elif args.stats:
        print_stats(conn)

    conn.close()


if __name__ == "__main__":
    main()
