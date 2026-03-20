"""
test_pipeline_units.py
Unit tests for pipeline.py logic that require NO network access.

Tests:
  - parse_13f_xml: all known namespace variants
  - parse_13f_xml: edge cases (empty, malformed, BOM prefix)
  - normalize_cik
  - accession_no cleaning (dash removal)
  - get_13f_xml_url HTML parsing logic (mocked fetch)
  - _text helper traversal
  - NS_MAP completeness
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Add parent directory so we can import pipeline
sys.path.insert(0, str(Path(__file__).parent.parent))

import pipeline


# ── Sample XML fixtures ────────────────────────────────────────────────────────

def _make_xml(namespace_uri: str, entries: list[dict]) -> bytes:
    """Build a minimal 13F information table XML with given namespace."""
    rows = ""
    for e in entries:
        rows += f"""
  <infoTable xmlns="{namespace_uri}">
    <nameOfIssuer>{e.get('issuer', 'TEST CORP')}</nameOfIssuer>
    <titleOfClass>{e.get('class', 'COM')}</titleOfClass>
    <cusip>{e.get('cusip', '037833100')}</cusip>
    <value>{e.get('value', '100000')}</value>
    <shrsOrPrnAmt>
      <sshPrnamt>{e.get('shares', '1000')}</sshPrnamt>
      <sshPrnamtType>{e.get('type', 'SH')}</sshPrnamtType>
    </shrsOrPrnAmt>
    <investmentDiscretion>{e.get('discretion', 'SOLE')}</investmentDiscretion>
    <votingAuthority>
      <Sole>{e.get('sole', '1000')}</Sole>
      <Shared>{e.get('shared', '0')}</Shared>
      <None>{e.get('none', '0')}</None>
    </votingAuthority>
  </infoTable>"""

    xml = f"""<?xml version="1.0" encoding="UTF-8"?>
<informationTable xmlns="{namespace_uri}">{rows}
</informationTable>"""
    return xml.encode("utf-8")


SAMPLE_ENTRY = [{"issuer": "APPLE INC", "cusip": "037833100", "value": "174000000",
                 "shares": "900000000", "type": "SH", "discretion": "SOLE",
                 "sole": "900000000", "shared": "0", "none": "0"}]


class TestParseXmlNamespaces(unittest.TestCase):
    """Ensure parser handles all known SEC 13F XML namespace variants."""

    def _parse(self, ns_uri):
        xml = _make_xml(ns_uri, SAMPLE_ENTRY)
        rows = pipeline.parse_13f_xml(xml, "0001067983", "2025-12-31", "0001234567-25-000001")
        return rows

    def test_namespace_ns3(self):
        """Modern namespace: thirteenf/informationtable"""
        rows = self._parse("http://www.sec.gov/edgar/document/thirteenf/informationtable")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["cusip"], "037833100")
        self.assertEqual(rows[0]["issuer_name"], "APPLE INC")

    def test_namespace_ns1(self):
        """Older namespace: 13f/informationtable"""
        rows = self._parse("http://www.sec.gov/edgar/document/13f/informationtable")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["issuer_name"], "APPLE INC")

    def test_namespace_ns2(self):
        """Alternate namespace: thirteenf/informationTable (capital T)"""
        rows = self._parse("http://www.sec.gov/edgar/thirteenf/informationTable")
        self.assertEqual(len(rows), 1)

    def test_namespace_n1(self):
        """Legacy namespace: urn:us:gov:sec:edgar"""
        rows = self._parse("urn:us:gov:sec:edgar:document:13f:information:v2")
        self.assertEqual(len(rows), 1)

    def test_unknown_namespace_handled(self):
        """Unknown namespace should still parse via dynamic NS detection."""
        rows = self._parse("http://www.sec.gov/edgar/future/unknownnamespace")
        # Should not crash — may return 0 or 1 rows depending on fallback
        self.assertIsInstance(rows, list)


class TestParseXmlFields(unittest.TestCase):
    def setUp(self):
        xml = _make_xml(
            "http://www.sec.gov/edgar/document/thirteenf/informationtable",
            SAMPLE_ENTRY
        )
        self.rows = pipeline.parse_13f_xml(xml, "0001067983", "2025-12-31", "0001234567-25-000001")
        self.row = self.rows[0]

    def test_cik_stored(self):
        self.assertEqual(self.row["cik"], "0001067983")

    def test_report_date_stored(self):
        self.assertEqual(self.row["report_date"], "2025-12-31")

    def test_accession_no_stored(self):
        self.assertEqual(self.row["accession_no"], "0001234567-25-000001")

    def test_issuer_name(self):
        self.assertEqual(self.row["issuer_name"], "APPLE INC")

    def test_cusip(self):
        self.assertEqual(self.row["cusip"], "037833100")

    def test_class_title(self):
        self.assertEqual(self.row["class_title"], "COM")

    def test_value_thousands_integer(self):
        self.assertIsInstance(self.row["value_thousands"], int)
        self.assertEqual(self.row["value_thousands"], 174000000)

    def test_shares_principal_integer(self):
        self.assertIsInstance(self.row["shares_principal"], int)
        self.assertEqual(self.row["shares_principal"], 900000000)

    def test_shares_type(self):
        self.assertEqual(self.row["shares_type"], "SH")

    def test_investment_discretion(self):
        self.assertEqual(self.row["investment_discretion"], "SOLE")

    def test_voting_sole(self):
        self.assertEqual(self.row["voting_sole"], 900000000)

    def test_voting_shared(self):
        self.assertEqual(self.row["voting_shared"], 0)

    def test_voting_none(self):
        self.assertEqual(self.row["voting_none"], 0)


class TestParseXmlEdgeCases(unittest.TestCase):

    def test_empty_bytes_returns_empty_list(self):
        rows = pipeline.parse_13f_xml(b"", "1234", "2025-12-31", "acc")
        self.assertEqual(rows, [])

    def test_malformed_xml_returns_empty_list(self):
        rows = pipeline.parse_13f_xml(b"<not valid xml>>>", "1234", "2025-12-31", "acc")
        self.assertEqual(rows, [])

    def test_bom_prefix_handled(self):
        xml = b"\xef\xbb\xbf" + _make_xml(
            "http://www.sec.gov/edgar/document/thirteenf/informationtable",
            SAMPLE_ENTRY
        )
        rows = pipeline.parse_13f_xml(xml, "0001067983", "2025-12-31", "acc-001")
        self.assertEqual(len(rows), 1)

    def test_multiple_entries_parsed(self):
        entries = [
            {"issuer": "APPLE INC", "cusip": "037833100", "value": "100"},
            {"issuer": "MICROSOFT", "cusip": "594918104", "value": "200"},
            {"issuer": "AMAZON",    "cusip": "023135106", "value": "300"},
        ]
        xml = _make_xml(
            "http://www.sec.gov/edgar/document/thirteenf/informationtable",
            entries
        )
        rows = pipeline.parse_13f_xml(xml, "1234", "2025-12-31", "acc")
        self.assertEqual(len(rows), 3)

    def test_zero_value_parsed(self):
        entry = [{"issuer": "TINY CORP", "cusip": "123456789", "value": "0", "shares": "0"}]
        xml = _make_xml(
            "http://www.sec.gov/edgar/document/thirteenf/informationtable",
            entry
        )
        rows = pipeline.parse_13f_xml(xml, "1234", "2025-12-31", "acc")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["value_thousands"], 0)


class TestNormalizeCik(unittest.TestCase):
    def test_short_cik_zero_padded(self):
        self.assertEqual(pipeline.normalize_cik("1067983"), "0001067983")

    def test_already_padded_cik(self):
        self.assertEqual(pipeline.normalize_cik("0001067983"), "0001067983")

    def test_leading_zeros_stripped_then_repadded(self):
        self.assertEqual(pipeline.normalize_cik("000102909"), "0000102909")


class TestAccessionNoCleaning(unittest.TestCase):
    """Verify accession number dash removal used in URL construction."""

    def test_dashes_removed(self):
        acc = "0001193125-26-054580"
        cleaned = acc.replace("-", "")
        self.assertEqual(cleaned, "000119312526054580")
        self.assertEqual(len(cleaned), 18)


class TestGetXmlUrlHtmlParsing(unittest.TestCase):
    """Test get_13f_xml_url with mocked fetch — no network calls."""

    MOCK_INDEX_HTML = b"""
    <html><body>
    <table class="tableFile">
    <tr>
      <td>13F-HR</td>
      <td><a href="/Archives/edgar/data/1067983/000119312526054580/xslForm13F_X02/primary_doc.xml">primary_doc.html</a></td>
      <td><a href="/Archives/edgar/data/1067983/000119312526054580/primary_doc.xml">primary_doc.xml</a></td>
      <td>FORM 13F COVER PAGE</td>
    </tr>
    <tr>
      <td>INFORMATION TABLE</td>
      <td><a href="/Archives/edgar/data/1067983/000119312526054580/xslForm13F_X02/50240.xml">50240.html</a></td>
      <td><a href="/Archives/edgar/data/1067983/000119312526054580/50240.xml">50240.xml</a></td>
      <td>INFORMATION TABLE</td>
    </tr>
    </table>
    </body></html>
    """

    def test_returns_information_table_xml_not_xslt(self):
        with patch.object(pipeline, "fetch", return_value=self.MOCK_INDEX_HTML):
            url = pipeline.get_13f_xml_url("0001067983", "0001193125-26-054580")
        self.assertIsNotNone(url)
        self.assertIn("50240.xml", url)
        self.assertNotIn("xslForm", url)

    def test_returns_www_sec_gov_not_data(self):
        with patch.object(pipeline, "fetch", return_value=self.MOCK_INDEX_HTML):
            url = pipeline.get_13f_xml_url("0001067983", "0001193125-26-054580")
        self.assertTrue(url.startswith("https://www.sec.gov"), f"URL should use www.sec.gov, got: {url}")

    def test_empty_index_returns_none(self):
        with patch.object(pipeline, "fetch", return_value=b""):
            url = pipeline.get_13f_xml_url("0001067983", "0001193125-26-054580")
        self.assertIsNone(url)

    def test_index_with_no_xml_returns_none(self):
        html_no_xml = b"<html><body><table><tr><td>13F-HR</td><td>nodocuments.htm</td></tr></table></body></html>"
        with patch.object(pipeline, "fetch", return_value=html_no_xml):
            url = pipeline.get_13f_xml_url("0001067983", "0001193125-26-054580")
        self.assertIsNone(url)


class TestNSMapCompleteness(unittest.TestCase):
    """Ensure all known SEC 13F namespaces are registered."""

    KNOWN_NAMESPACES = [
        "http://www.sec.gov/edgar/document/thirteenf/informationtable",
        "http://www.sec.gov/edgar/document/13f/informationtable",
        "http://www.sec.gov/edgar/thirteenf/informationTable",
        "urn:us:gov:sec:edgar:document:13f:information:v2",
    ]

    def test_all_known_namespaces_in_ns_map(self):
        registered = set(pipeline.NS_MAP.values())
        for ns in self.KNOWN_NAMESPACES:
            self.assertIn(ns, registered, f"Namespace not in NS_MAP: {ns}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
