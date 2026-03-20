"""
test_data_quality.py
Validates the content of edgar_13f.db against Phase 1 data quality rules.

Checks:
  - Required fields are non-null
  - Date formats are YYYY-MM-DD
  - report_date values are valid quarter-end dates
  - form_type values are in allowed set
  - amendment is 0 or 1
  - CUSIP is exactly 9 characters
  - value_thousands and shares_principal are non-negative
  - shares_type is SH or PRN
  - investment_discretion is SOLE, DFND, or OTR
  - No duplicate filings in filers table
  - All holdings link to an existing filer (referential integrity)
  - Database is not empty
"""

import re
import sqlite3
import unittest
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "edgar_13f.db"

VALID_QUARTER_ENDS = {
    "03-31", "06-30", "09-30", "12-31"
}

VALID_FORM_TYPES = {"13F-HR", "13F-HR/A"}
VALID_SHARES_TYPES = {"SH", "PRN"}
VALID_DISCRETION = {"SOLE", "DFND", "OTR"}
ACCESSION_RE = re.compile(r"^\d{10}-\d{2}-\d{6}$")
CUSIP_RE = re.compile(r"^[A-Z0-9]{9}$", re.IGNORECASE)
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def get_conn():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. Run pipeline.py first."
        )
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


class TestDatabaseNotEmpty(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()

    def tearDown(self):
        self.conn.close()

    def test_filers_has_rows(self):
        n = self.conn.execute("SELECT COUNT(*) FROM filers").fetchone()[0]
        self.assertGreater(n, 0, "filers table is empty — run pipeline.py first")

    def test_holdings_has_rows(self):
        n = self.conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
        self.assertGreater(n, 0, "holdings table is empty — run pipeline.py first")


class TestFilersDataQuality(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()
        self.filers = self.conn.execute("SELECT * FROM filers").fetchall()

    def tearDown(self):
        self.conn.close()

    def test_no_null_cik(self):
        bad = [r["accession_no"] for r in self.filers if not r["cik"]]
        self.assertFalse(bad, f"NULL cik in filers: {bad[:5]}")

    def test_no_null_accession_no(self):
        bad = [r["cik"] for r in self.filers if not r["accession_no"]]
        self.assertFalse(bad, f"NULL accession_no in filers: {bad[:5]}")

    def test_cik_is_10_digits(self):
        bad = [r["cik"] for r in self.filers if not re.match(r"^\d{10}$", str(r["cik"] or ""))]
        self.assertFalse(bad, f"CIK not 10 digits: {bad[:5]}")

    def test_accession_no_format(self):
        bad = [r["accession_no"] for r in self.filers
               if not ACCESSION_RE.match(str(r["accession_no"] or ""))]
        self.assertFalse(bad, f"Bad accession_no format: {bad[:5]}")

    def test_form_type_valid(self):
        bad = [r["accession_no"] for r in self.filers
               if r["form_type"] not in VALID_FORM_TYPES]
        self.assertFalse(bad, f"Invalid form_type values: {bad[:5]}")

    def test_amendment_is_0_or_1(self):
        bad = [r["accession_no"] for r in self.filers
               if r["amendment"] not in (0, 1)]
        self.assertFalse(bad, f"Invalid amendment values: {bad[:5]}")

    def test_filing_date_format(self):
        bad = [r["accession_no"] for r in self.filers
               if r["filing_date"] and not DATE_RE.match(str(r["filing_date"]))]
        self.assertFalse(bad, f"Bad filing_date format: {bad[:5]}")

    def test_report_date_format(self):
        bad = [r["accession_no"] for r in self.filers
               if r["report_date"] and not DATE_RE.match(str(r["report_date"]))]
        self.assertFalse(bad, f"Bad report_date format: {bad[:5]}")

    def test_report_date_is_quarter_end(self):
        bad = []
        for r in self.filers:
            rd = str(r["report_date"] or "")
            if DATE_RE.match(rd):
                mm_dd = rd[5:]  # MM-DD
                if mm_dd not in VALID_QUARTER_ENDS:
                    bad.append((r["accession_no"], rd))
        self.assertFalse(bad, f"report_date not a quarter-end: {bad[:5]}")

    def test_filing_date_after_report_date(self):
        bad = []
        for r in self.filers:
            fd = str(r["filing_date"] or "")
            rd = str(r["report_date"] or "")
            if DATE_RE.match(fd) and DATE_RE.match(rd):
                if fd < rd:
                    bad.append((r["accession_no"], f"filed={fd} report={rd}"))
        self.assertFalse(bad, f"filing_date before report_date: {bad[:5]}")

    def test_no_duplicate_filings(self):
        cur = self.conn.execute("""
            SELECT cik, accession_no, COUNT(*) AS cnt
            FROM filers
            GROUP BY cik, accession_no
            HAVING cnt > 1
        """)
        dups = cur.fetchall()
        self.assertFalse(dups, f"Duplicate (cik, accession_no) in filers: {list(dups)[:5]}")

    def test_amendment_flag_matches_form_type(self):
        bad = []
        for r in self.filers:
            is_amend_form = "/A" in str(r["form_type"] or "")
            flag = r["amendment"]
            if is_amend_form and flag != 1:
                bad.append(r["accession_no"])
            elif not is_amend_form and flag != 0:
                bad.append(r["accession_no"])
        self.assertFalse(bad, f"amendment flag mismatch with form_type: {bad[:5]}")


class TestHoldingsDataQuality(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()
        self.holdingsList = self.conn.execute("SELECT * FROM holdings LIMIT 5000").fetchall()

    def tearDown(self):
        self.conn.close()

    def test_no_null_cik(self):
        bad = [r["id"] for r in self.holdingsList if not r["cik"]]
        self.assertFalse(bad, f"NULL cik in holdings: {bad[:5]}")

    def test_no_null_report_date(self):
        bad = [r["id"] for r in self.holdingsList if not r["report_date"]]
        self.assertFalse(bad, f"NULL report_date in holdings: {bad[:5]}")

    def test_no_null_accession_no(self):
        bad = [r["id"] for r in self.holdingsList if not r["accession_no"]]
        self.assertFalse(bad, f"NULL accession_no in holdings: {bad[:5]}")

    def test_cusip_format(self):
        bad = [(r["id"], r["cusip"]) for r in self.holdingsList
               if r["cusip"] and not CUSIP_RE.match(str(r["cusip"]))]
        self.assertFalse(bad, f"Invalid CUSIP format (should be 9 alphanumeric): {bad[:5]}")

    def test_cusip_length_9(self):
        bad = [(r["id"], r["cusip"]) for r in self.holdingsList
               if r["cusip"] and len(str(r["cusip"])) != 9]
        self.assertFalse(bad, f"CUSIP not 9 chars: {bad[:5]}")

    def test_value_thousands_non_negative(self):
        bad = [(r["id"], r["value_thousands"]) for r in self.holdingsList
               if r["value_thousands"] is not None and r["value_thousands"] < 0]
        self.assertFalse(bad, f"Negative value_thousands: {bad[:5]}")

    def test_shares_principal_non_negative(self):
        bad = [(r["id"], r["shares_principal"]) for r in self.holdingsList
               if r["shares_principal"] is not None and r["shares_principal"] < 0]
        self.assertFalse(bad, f"Negative shares_principal: {bad[:5]}")

    def test_shares_type_valid(self):
        bad = [(r["id"], r["shares_type"]) for r in self.holdingsList
               if r["shares_type"] and r["shares_type"] not in VALID_SHARES_TYPES]
        self.assertFalse(bad, f"Invalid shares_type (expected SH or PRN): {bad[:5]}")

    def test_investment_discretion_valid(self):
        bad = [(r["id"], r["investment_discretion"]) for r in self.holdingsList
               if r["investment_discretion"]
               and r["investment_discretion"] not in VALID_DISCRETION]
        self.assertFalse(bad, f"Invalid investment_discretion: {bad[:5]}")

    def test_put_call_valid(self):
        valid = {"Put", "Call", "put", "call", "PUT", "CALL", None, ""}
        bad = [(r["id"], r["put_call"]) for r in self.holdingsList
               if r["put_call"] and r["put_call"] not in valid]
        self.assertFalse(bad, f"Invalid put_call value: {bad[:5]}")

    def test_report_date_is_quarter_end(self):
        bad = []
        for r in self.holdingsList:
            rd = str(r["report_date"] or "")
            if DATE_RE.match(rd) and rd[5:] not in VALID_QUARTER_ENDS:
                bad.append((r["id"], rd))
        self.assertFalse(bad, f"holdings.report_date not quarter-end: {bad[:5]}")

    def test_no_all_null_key_fields(self):
        bad = [r["id"] for r in self.holdingsList
               if not r["cusip"] and not r["issuer_name"]]
        self.assertFalse(bad, f"Holdings with both cusip and issuer_name NULL: {bad[:5]}")


class TestReferentialIntegrity(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()

    def tearDown(self):
        self.conn.close()

    def test_all_holdings_have_matching_filer(self):
        cur = self.conn.execute("""
            SELECT h.id, h.cik, h.accession_no
            FROM holdings h
            LEFT JOIN filers f ON f.cik = h.cik AND f.accession_no = h.accession_no
            WHERE f.cik IS NULL
            LIMIT 10
        """)
        orphans = cur.fetchall()
        self.assertFalse(
            orphans,
            f"Holdings with no matching filer (orphaned rows): {[dict(r) for r in orphans]}"
        )

    def test_holdings_report_date_matches_filer(self):
        cur = self.conn.execute("""
            SELECT h.id, h.cik, h.accession_no,
                   h.report_date AS h_date, f.report_date AS f_date
            FROM holdings h
            JOIN filers f ON f.cik = h.cik AND f.accession_no = h.accession_no
            WHERE h.report_date != f.report_date
            LIMIT 10
        """)
        mismatches = cur.fetchall()
        self.assertFalse(
            mismatches,
            f"report_date mismatch between holdings and filers: {[dict(r) for r in mismatches]}"
        )


class TestCoverageStats(unittest.TestCase):
    """Soft checks — warns if coverage looks unexpectedly thin."""

    def setUp(self):
        self.conn = get_conn()

    def tearDown(self):
        self.conn.close()

    def test_at_least_one_manager(self):
        n = self.conn.execute("SELECT COUNT(DISTINCT cik) FROM filers").fetchone()[0]
        self.assertGreaterEqual(n, 1, "Expected at least 1 manager in filers")

    def test_holdings_per_filing_reasonable(self):
        cur = self.conn.execute("""
            SELECT f.accession_no, COUNT(h.id) AS n
            FROM filers f
            JOIN holdings h ON h.accession_no = f.accession_no AND h.cik = f.cik
            GROUP BY f.accession_no
            HAVING n < 1
        """)
        empty_filings = cur.fetchall()
        self.assertFalse(
            empty_filings,
            f"Filings with 0 holdings (suspicious): {[r[0] for r in empty_filings]}"
        )

    def test_total_aum_is_positive(self):
        total = self.conn.execute(
            "SELECT SUM(value_thousands) FROM holdings"
        ).fetchone()[0]
        self.assertIsNotNone(total, "No value_thousands data at all")
        self.assertGreater(total, 0, "Total AUM is zero or negative")


if __name__ == "__main__":
    unittest.main(verbosity=2)
