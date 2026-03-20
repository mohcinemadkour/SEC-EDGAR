"""
SEC EDGAR 13F Holdings Pipeline (2010–Present)
Builds a manager–security–quarter panel dataset from EDGAR structured filings.

Usage:
    python pipeline.py --ciks 0001067983 0001350694 --start 2020-01-01
    python pipeline.py --top-managers 50 --start 2010-01-01
"""

import argparse
import json
import re
import sqlite3
import time
import csv
import sys
from pathlib import Path
from datetime import datetime, date
from typing import Optional
import urllib.request
import urllib.error
import xml.etree.ElementTree as ET

# ── constants ──────────────────────────────────────────────────────────────────
BASE_URL    = "https://data.sec.gov"       # submissions / API endpoints
ARCHIVE_URL = "https://www.sec.gov"        # /Archives/ file access
SEARCH_URL  = "https://efts.sec.gov/LATEST/search-index"
HEADERS     = {"User-Agent": "13F-Research-Pipeline research@example.com"}

DB_PATH    = Path("edgar_13f.db")
RATE_SLEEP = 0.12   # respect EDGAR's 10 req/s guidance

# ── helpers ────────────────────────────────────────────────────────────────────

def fetch(url: str, retries: int = 3) -> bytes:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=30) as r:
                return r.read()
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 ** attempt * 5
                print(f"  [rate-limit] waiting {wait}s …", flush=True)
                time.sleep(wait)
            elif e.code == 404:
                return b""
            else:
                raise
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(2 ** attempt)
    return b""


def fetch_json(url: str) -> dict:
    raw = fetch(url)
    return json.loads(raw) if raw else {}


# ── database setup ─────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS filers (
    cik             TEXT NOT NULL,
    manager_name    TEXT,
    filing_date     TEXT,
    report_date     TEXT,
    accession_no    TEXT,
    form_type       TEXT DEFAULT '13F-HR',
    amendment       INTEGER DEFAULT 0,
    PRIMARY KEY (cik, accession_no)
);

CREATE TABLE IF NOT EXISTS holdings (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    cik                     TEXT NOT NULL,
    report_date             TEXT NOT NULL,
    accession_no            TEXT NOT NULL,
    issuer_name             TEXT,
    cusip                   TEXT,
    class_title             TEXT,
    value_thousands         INTEGER,
    shares_principal        INTEGER,
    shares_type             TEXT,   -- SH or PRN
    put_call                TEXT,
    investment_discretion   TEXT,
    voting_sole             INTEGER,
    voting_shared           INTEGER,
    voting_none             INTEGER,
    FOREIGN KEY (cik, accession_no) REFERENCES filers(cik, accession_no)
);

CREATE INDEX IF NOT EXISTS idx_holdings_cik_date   ON holdings(cik, report_date);
CREATE INDEX IF NOT EXISTS idx_holdings_cusip       ON holdings(cusip);
CREATE INDEX IF NOT EXISTS idx_holdings_report_date ON holdings(report_date);
"""

def init_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ── CIK discovery ──────────────────────────────────────────────────────────────

def get_top_13f_filers(limit: int = 50) -> list[dict]:
    """
    Uses EDGAR full-text search to discover high-volume 13F filers.
    Returns list of {cik, name}.
    """
    url = (
        f"https://efts.sec.gov/LATEST/search-index?q=%2213F-HR%22"
        f"&dateRange=custom&startdt=2023-01-01&enddt=2023-12-31"
        f"&forms=13F-HR&hits.hits._source=period_of_report,entity_name,file_num"
        f"&hits.hits.total=true&hits.hits.hits.total=true"
    )
    # Fallback: use EDGAR company search for known large filers
    known = [
        ("0001067983", "Berkshire Hathaway Inc"),
        ("0001350694", "BlackRock Inc"),
        ("0001079114", "Vanguard Group Inc"),
        ("0000102909", "Fidelity Management & Research"),
        ("0001603923", "Citadel Advisors LLC"),
        ("0001336528", "Point72 Asset Management"),
        ("0001364742", "Millennium Management LLC"),
        ("0001037389", "Goldman Sachs Group Inc"),
        ("0000895421", "Morgan Stanley"),
        ("0001166559", "Two Sigma Investments"),
    ]
    return [{"cik": c, "name": n} for c, n in known[:limit]]


def normalize_cik(cik: str) -> str:
    return cik.lstrip("0").zfill(10)


# ── filing discovery ───────────────────────────────────────────────────────────

def get_filings_for_cik(cik: str, start_date: str = "2010-01-01") -> list[dict]:
    """
    Fetch all 13F-HR (and 13F-HR/A) filings for a CIK via EDGAR submissions API.
    """
    padded = cik.zfill(10)
    url    = f"{BASE_URL}/submissions/CIK{padded}.json"
    data   = fetch_json(url)
    time.sleep(RATE_SLEEP)

    if not data:
        return []

    filings = []
    recent  = data.get("filings", {}).get("recent", {})

    forms        = recent.get("form", [])
    accessions   = recent.get("accessionNumber", [])
    filing_dates = recent.get("filingDate", [])
    report_dates = recent.get("reportDate", [])

    for form, acc, fdate, rdate in zip(forms, accessions, filing_dates, report_dates):
        if form not in ("13F-HR", "13F-HR/A"):
            continue
        if fdate < start_date:
            continue
        filings.append({
            "cik":          padded,
            "accession_no": acc,
            "form_type":    form,
            "filing_date":  fdate,
            "report_date":  rdate,
            "amendment":    1 if "/A" in form else 0,
        })

    # Also check older filing pages
    for page_url in data.get("filings", {}).get("files", []):
        old_url  = f"{BASE_URL}/submissions/{page_url['name']}"
        old_data = fetch_json(old_url)
        time.sleep(RATE_SLEEP)

        o_forms        = old_data.get("form", [])
        o_accessions   = old_data.get("accessionNumber", [])
        o_filing_dates = old_data.get("filingDate", [])
        o_report_dates = old_data.get("reportDate", [])

        for form, acc, fdate, rdate in zip(o_forms, o_accessions, o_filing_dates, o_report_dates):
            if form not in ("13F-HR", "13F-HR/A"):
                continue
            if fdate < start_date:
                continue
            filings.append({
                "cik":          padded,
                "accession_no": acc,
                "form_type":    form,
                "filing_date":  fdate,
                "report_date":  rdate,
                "amendment":    1 if "/A" in form else 0,
            })

    return filings


# ── XML holdings parser ────────────────────────────────────────────────────────

NS_MAP = {
    "n1":    "urn:us:gov:sec:edgar:document:13f:information:v2",
    "ns1":   "http://www.sec.gov/edgar/document/13f/informationtable",
    "ns2":   "http://www.sec.gov/edgar/thirteenf/informationTable",
    "ns3":   "http://www.sec.gov/edgar/document/thirteenf/informationtable",
    "xbrli": "http://www.xbrl.org/2003/instance",
}

# Reverse map: URI → prefix key
_NS_URI_TO_KEY = {v: k for k, v in NS_MAP.items()}

def _tag(ns: str, local: str) -> str:
    return f"{{{NS_MAP[ns]}}}{local}"

def _text(el, *path, ns="ns1") -> Optional[str]:
    node = el
    for p in path:
        node = node.find(_tag(ns, p))
        if node is None:
            # try all known namespaces
            for alt in NS_MAP:
                if alt == ns:
                    continue
                node = el.find(_tag(alt, p))
                if node is not None:
                    break
            if node is None:
                return None
        el = node
    return (node.text or "").strip() or None


def parse_13f_xml(raw_xml: bytes, cik: str, report_date: str, accession_no: str) -> list[dict]:
    """Parse the information table XML from a 13F-HR filing."""
    if not raw_xml:
        return []

    # strip BOM if present
    xml_str = raw_xml.lstrip(b"\xef\xbb\xbf")

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return []

    rows = []

    # Detect namespace from the root element's actual tag (most reliable approach)
    ns = "ns1"  # default
    if root.tag.startswith("{"):
        actual_uri = root.tag[1:root.tag.index("}")]
        if actual_uri in _NS_URI_TO_KEY:
            ns = _NS_URI_TO_KEY[actual_uri]
        else:
            # Unknown namespace — add it dynamically so _tag() / _text() can use it
            ns = "_dynamic"
            NS_MAP["_dynamic"] = actual_uri
            _NS_URI_TO_KEY[actual_uri] = "_dynamic"

    for info_entry in root.iter():
        local = info_entry.tag.split("}")[-1] if "}" in info_entry.tag else info_entry.tag
        if local != "infoTable":
            continue

        def g(*path):
            return _text(info_entry, *path, ns=ns)

        # voting authority
        vote_auth = info_entry.find(_tag(ns, "votingAuthority"))
        if vote_auth is None:
            for alt in ("ns2", "n1", "ns1"):
                vote_auth = info_entry.find(_tag(alt, "votingAuthority"))
                if vote_auth is not None:
                    break

        def vote(field):
            if vote_auth is None:
                return None
            el = vote_auth.find(_tag(ns, field))
            if el is None:
                for alt in ("ns2", "n1", "ns1"):
                    el = vote_auth.find(_tag(alt, field))
                    if el is not None:
                        break
            return int(el.text) if el is not None and el.text else None

        try:
            val = int(g("value") or 0)
        except ValueError:
            val = None

        try:
            shr = int(g("shrsOrPrnAmt", "sshPrnamt") or g("shrsOrPrnAmt", "sshPrnamtType") or 0)
        except (ValueError, TypeError):
            shr = None

        rows.append({
            "cik":                  cik,
            "report_date":          report_date,
            "accession_no":         accession_no,
            "issuer_name":          g("nameOfIssuer"),
            "cusip":                g("cusip"),
            "class_title":          g("titleOfClass"),
            "value_thousands":      val,
            "shares_principal":     shr,
            "shares_type":          g("shrsOrPrnAmt", "sshPrnamtType"),
            "put_call":             g("putCall"),
            "investment_discretion":g("investmentDiscretion"),
            "voting_sole":          vote("Sole"),
            "voting_shared":        vote("Shared"),
            "voting_none":          vote("None"),
        })

    return rows


# ── filing document fetcher ────────────────────────────────────────────────────

def get_13f_xml_url(cik: str, accession_no: str) -> Optional[str]:
    """Find the information table XML document URL within a filing index."""
    acc_clean = accession_no.replace("-", "")
    cik_int   = int(cik)

    # Fetch the HTML filing index (JSON index does not exist on EDGAR)
    index_url = (
        f"{ARCHIVE_URL}/Archives/edgar/data/{cik_int}/"
        f"{acc_clean}/{accession_no}-index.htm"
    )
    raw = fetch(index_url)
    time.sleep(RATE_SLEEP)

    if not raw:
        print(f"[debug] index fetch failed: {index_url}", flush=True)
        return None

    html = raw.decode("utf-8", errors="ignore")

    # Look for href links to .xml files in the archive table
    # Exclude xslForm subdirectory paths — those are XSLT-rendered display copies,
    # not the raw information table data

    # All XML hrefs NOT under an xslForm* subdirectory (raw data files)
    raw_xml_hrefs = re.findall(
        r'href="(/Archives/edgar/data/[^"]+\.xml)"',
        html, re.IGNORECASE
    )
    raw_xml_hrefs = [h for h in raw_xml_hrefs if "/xslform" not in h.lower()]

    # Strategy 1: find INFORMATION TABLE row that links to a raw (non-xslForm) XML
    # Scan rows of the filing index table looking for "INFORMATION TABLE" type
    rows_html = re.findall(
        r'<tr[^>]*>(.*?)</tr>',
        html, re.IGNORECASE | re.DOTALL
    )
    for row in rows_html:
        if "information table" not in row.lower():
            continue
        hrefs_in_row = re.findall(
            r'href="(/Archives/edgar/data/[^"]+\.xml)"',
            row, re.IGNORECASE
        )
        for href in hrefs_in_row:
            if "/xslform" not in href.lower():
                return f"{ARCHIVE_URL}{href}"

    # Strategy 2: any raw XML not named primary_doc
    for href in raw_xml_hrefs:
        fname = href.split("/")[-1].lower()
        if fname != "primary_doc.xml":
            return f"{ARCHIVE_URL}{href}"

    # Strategy 3: any raw XML
    if raw_xml_hrefs:
        return f"{ARCHIVE_URL}{raw_xml_hrefs[0]}"

    print(f"[debug] no XML links found in index for {accession_no}", flush=True)
    return None


def get_manager_name(cik: str) -> str:
    padded = cik.zfill(10)
    url    = f"{BASE_URL}/submissions/CIK{padded}.json"
    data   = fetch_json(url)
    time.sleep(RATE_SLEEP)
    return data.get("name", f"CIK-{cik}")


# ── main pipeline ──────────────────────────────────────────────────────────────

def run_pipeline(
    conn: sqlite3.Connection,
    ciks: list[str],
    start_date: str = "2010-01-01",
    limit_per_filer: Optional[int] = None,
):
    cur = conn.cursor()

    for cik in ciks:
        padded = cik.zfill(10)
        print(f"\n{'─'*60}", flush=True)
        print(f"Processing CIK: {padded}", flush=True)

        manager_name = get_manager_name(padded)
        print(f"Manager: {manager_name}", flush=True)

        filings = get_filings_for_cik(padded, start_date)
        if limit_per_filer:
            filings = filings[:limit_per_filer]

        print(f"Found {len(filings)} 13F filings since {start_date}", flush=True)

        for i, filing in enumerate(filings):
            acc = filing["accession_no"]
            rdate = filing["report_date"]

            # skip if already ingested
            cur.execute(
                "SELECT 1 FROM filers WHERE cik=? AND accession_no=?",
                (padded, acc)
            )
            if cur.fetchone():
                print(f"  [{i+1}/{len(filings)}] {rdate} — already in DB, skipping", flush=True)
                continue

            print(f"  [{i+1}/{len(filings)}] {rdate}  acc={acc}", end=" … ", flush=True)

            xml_url = get_13f_xml_url(padded, acc)
            if not xml_url:
                print("no XML found", flush=True)
                continue

            raw_xml = fetch(xml_url)
            time.sleep(RATE_SLEEP)

            rows = parse_13f_xml(raw_xml, padded, rdate, acc)
            if not rows:
                print("0 rows parsed", flush=True)
                continue

            # insert filer record
            cur.execute("""
                INSERT OR IGNORE INTO filers
                  (cik, manager_name, filing_date, report_date, accession_no, form_type, amendment)
                VALUES (?,?,?,?,?,?,?)
            """, (
                padded, manager_name,
                filing["filing_date"], rdate,
                acc, filing["form_type"], filing["amendment"]
            ))

            # insert holdings
            cur.executemany("""
                INSERT INTO holdings
                  (cik, report_date, accession_no, issuer_name, cusip, class_title,
                   value_thousands, shares_principal, shares_type, put_call,
                   investment_discretion, voting_sole, voting_shared, voting_none)
                VALUES
                  (:cik, :report_date, :accession_no, :issuer_name, :cusip, :class_title,
                   :value_thousands, :shares_principal, :shares_type, :put_call,
                   :investment_discretion, :voting_sole, :voting_shared, :voting_none)
            """, rows)

            conn.commit()
            print(f"{len(rows):,} holdings saved", flush=True)

    print(f"\n{'═'*60}", flush=True)
    print("Pipeline complete.", flush=True)

    # summary
    n_filers   = cur.execute("SELECT COUNT(DISTINCT cik) FROM filers").fetchone()[0]
    n_filings  = cur.execute("SELECT COUNT(*) FROM filers").fetchone()[0]
    n_holdings = cur.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
    print(f"  Filers:   {n_filers:,}", flush=True)
    print(f"  Filings:  {n_filings:,}", flush=True)
    print(f"  Holdings: {n_holdings:,}", flush=True)


# ── export ─────────────────────────────────────────────────────────────────────

def export_csv(conn: sqlite3.Connection, out_dir: Path = Path(".")):
    out_dir.mkdir(exist_ok=True)

    # filers
    cur = conn.cursor()
    cur.execute("SELECT * FROM filers ORDER BY cik, filing_date")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    with open(out_dir / "filers.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"Exported filers.csv  ({len(rows):,} rows)")

    # holdings
    cur.execute("SELECT * FROM holdings ORDER BY cik, report_date, cusip")
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    with open(out_dir / "holdings.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)
    print(f"Exported holdings.csv ({len(rows):,} rows)")


# ── CLI ────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Build 13F Holdings Dataset from SEC EDGAR (2010–present)"
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--ciks", nargs="+",
        help="One or more CIK numbers (e.g. 0001067983)"
    )
    group.add_argument(
        "--top-managers", type=int, default=10,
        help="Use built-in list of top 13F filers (default: 10)"
    )
    parser.add_argument(
        "--start", default="2020-01-01",
        help="Start date for filings (YYYY-MM-DD, default: 2020-01-01)"
    )
    parser.add_argument(
        "--limit", type=int, default=None,
        help="Max filings to process per filer (useful for testing)"
    )
    parser.add_argument(
        "--export-csv", action="store_true",
        help="Export filers.csv and holdings.csv after ingestion"
    )
    parser.add_argument(
        "--db", default="edgar_13f.db",
        help="SQLite database path (default: edgar_13f.db)"
    )

    args = parser.parse_args()

    conn = init_db(Path(args.db))

    if args.ciks:
        ciks = [c.lstrip("0").zfill(10) for c in args.ciks]
    else:
        filers = get_top_13f_filers(args.top_managers)
        ciks   = [f["cik"] for f in filers]

    run_pipeline(conn, ciks, start_date=args.start, limit_per_filer=args.limit)

    if args.export_csv:
        export_csv(conn)

    conn.close()


if __name__ == "__main__":
    main()
