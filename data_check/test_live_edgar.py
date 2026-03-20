"""
test_live_edgar.py
Integration tests that make REAL network calls to SEC EDGAR.

These are kept minimal and rate-limited to avoid hammering the API.
Run sparingly — not on every CI check.

Tests:
  - EDGAR submissions API is reachable
  - get_filings_for_cik returns valid 13F filings for Berkshire
  - get_13f_xml_url resolves a known accession number
  - parse_13f_xml produces holdings from a live filing
  - End-to-end: fetch → URL → parse produces correct fields
"""

import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pipeline

# Known stable test fixtures (Berkshire Hathaway Q3 2025)
TEST_CIK          = "0001067983"
TEST_ACCESSION    = "0001193125-25-282901"   # 13F-HR Q3 2025
TEST_REPORT_DATE  = "2025-09-30"
RATE_SLEEP        = 0.5   # be polite during tests


class TestEdgarReachable(unittest.TestCase):
    def test_submissions_api_returns_data(self):
        data = pipeline.fetch_json(
            f"https://data.sec.gov/submissions/CIK{TEST_CIK}.json"
        )
        time.sleep(RATE_SLEEP)
        self.assertIsInstance(data, dict, "Expected a JSON dict from submissions API")
        self.assertIn("name", data, "Response missing 'name' field")
        self.assertIn("filings", data, "Response missing 'filings' field")

    def test_manager_name_resolves(self):
        name = pipeline.get_manager_name(TEST_CIK)
        time.sleep(RATE_SLEEP)
        self.assertIsInstance(name, str)
        self.assertGreater(len(name), 0)
        self.assertIn("BERKSHIRE", name.upper())


class TestFilingDiscovery(unittest.TestCase):
    def test_get_filings_returns_list(self):
        filings = pipeline.get_filings_for_cik(TEST_CIK, start_date="2025-01-01")
        time.sleep(RATE_SLEEP)
        self.assertIsInstance(filings, list)
        self.assertGreater(len(filings), 0, "Expected at least 1 filing since 2025-01-01")

    def test_filing_has_required_fields(self):
        filings = pipeline.get_filings_for_cik(TEST_CIK, start_date="2025-01-01")
        time.sleep(RATE_SLEEP)
        required = {"cik", "accession_no", "form_type", "filing_date", "report_date", "amendment"}
        for f in filings:
            missing = required - set(f.keys())
            self.assertFalse(missing, f"Filing missing fields: {missing}")

    def test_only_13f_forms_returned(self):
        filings = pipeline.get_filings_for_cik(TEST_CIK, start_date="2025-01-01")
        time.sleep(RATE_SLEEP)
        bad = [f["form_type"] for f in filings if f["form_type"] not in ("13F-HR", "13F-HR/A")]
        self.assertFalse(bad, f"Non-13F forms returned: {bad}")

    def test_cik_is_padded_to_10_digits(self):
        filings = pipeline.get_filings_for_cik(TEST_CIK, start_date="2025-01-01")
        time.sleep(RATE_SLEEP)
        for f in filings:
            self.assertEqual(len(f["cik"]), 10, f"CIK not 10 digits: {f['cik']}")


class TestXmlUrlResolution(unittest.TestCase):
    def test_known_accession_resolves_url(self):
        url = pipeline.get_13f_xml_url(TEST_CIK, TEST_ACCESSION)
        time.sleep(RATE_SLEEP)
        self.assertIsNotNone(url, f"Expected XML URL for {TEST_ACCESSION}, got None")
        self.assertTrue(url.startswith("https://www.sec.gov"), f"Wrong base URL: {url}")
        self.assertTrue(url.endswith(".xml"), f"URL doesn't end in .xml: {url}")

    def test_url_does_not_contain_xslform(self):
        url = pipeline.get_13f_xml_url(TEST_CIK, TEST_ACCESSION)
        time.sleep(RATE_SLEEP)
        if url:
            self.assertNotIn("xslform", url.lower(),
                             "URL points to XSLT display copy, not raw XML")


class TestEndToEndParsing(unittest.TestCase):
    """Fetch a real filing and parse it — full pipeline smoke test."""

    @classmethod
    def setUpClass(cls):
        url = pipeline.get_13f_xml_url(TEST_CIK, TEST_ACCESSION)
        time.sleep(RATE_SLEEP)
        if not url:
            cls.rows = []
            return
        raw = pipeline.fetch(url)
        time.sleep(RATE_SLEEP)
        cls.rows = pipeline.parse_13f_xml(raw, TEST_CIK, TEST_REPORT_DATE, TEST_ACCESSION)

    def test_rows_not_empty(self):
        self.assertGreater(len(self.rows), 0,
                           "parse_13f_xml returned 0 rows for a real Berkshire filing")

    def test_all_rows_have_cusip(self):
        bad = [r for r in self.rows if not r.get("cusip")]
        self.assertFalse(bad, f"{len(bad)} rows missing CUSIP")

    def test_all_rows_have_issuer_name(self):
        bad = [r for r in self.rows if not r.get("issuer_name")]
        self.assertFalse(bad, f"{len(bad)} rows missing issuer_name")

    def test_value_thousands_positive(self):
        bad = [r for r in self.rows
               if r.get("value_thousands") is not None and r["value_thousands"] < 0]
        self.assertFalse(bad, f"{len(bad)} rows with negative value_thousands")

    def test_cik_correct(self):
        bad = [r for r in self.rows if r["cik"] != TEST_CIK]
        self.assertFalse(bad, "Some rows have wrong CIK")

    def test_report_date_correct(self):
        bad = [r for r in self.rows if r["report_date"] != TEST_REPORT_DATE]
        self.assertFalse(bad, "Some rows have wrong report_date")

    def test_shares_type_valid(self):
        bad = [(r["cusip"], r["shares_type"]) for r in self.rows
               if r.get("shares_type") and r["shares_type"] not in ("SH", "PRN")]
        self.assertFalse(bad, f"Invalid shares_type in real filing: {bad[:5]}")

    def test_investment_discretion_valid(self):
        valid = {"SOLE", "DFND", "OTR"}
        bad = [r["cusip"] for r in self.rows
               if r.get("investment_discretion") and r["investment_discretion"] not in valid]
        self.assertFalse(bad, f"Invalid investment_discretion: {bad[:5]}")

    def test_row_count_reasonable(self):
        """Berkshire typically reports 40–150 holdings."""
        self.assertGreaterEqual(len(self.rows), 10,
                                f"Suspiciously few holdings: {len(self.rows)}")
        self.assertLessEqual(len(self.rows), 1000,
                             f"Suspiciously many holdings: {len(self.rows)}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
