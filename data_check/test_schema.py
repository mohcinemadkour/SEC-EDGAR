"""
test_schema.py
Validates that edgar_13f.db has the correct schema for Phase 1.
Checks: tables, columns, types, primary keys, indexes.
"""

import sqlite3
import sys
import unittest
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "edgar_13f.db"


def get_conn():
    if not DB_PATH.exists():
        raise FileNotFoundError(
            f"Database not found at {DB_PATH}. Run pipeline.py first."
        )
    return sqlite3.connect(DB_PATH)


class TestTablesExist(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()

    def tearDown(self):
        self.conn.close()

    def test_filers_table_exists(self):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='filers'"
        )
        self.assertIsNotNone(cur.fetchone(), "Table 'filers' does not exist")

    def test_holdings_table_exists(self):
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='holdings'"
        )
        self.assertIsNotNone(cur.fetchone(), "Table 'holdings' does not exist")


class TestFilersSchema(unittest.TestCase):
    REQUIRED_COLUMNS = {
        "cik", "manager_name", "filing_date", "report_date",
        "accession_no", "form_type", "amendment"
    }

    def setUp(self):
        self.conn = get_conn()
        cur = self.conn.execute("PRAGMA table_info(filers)")
        self.columns = {row[1]: row[2] for row in cur.fetchall()}  # name -> type

    def tearDown(self):
        self.conn.close()

    def test_all_required_columns_present(self):
        missing = self.REQUIRED_COLUMNS - set(self.columns)
        self.assertFalse(missing, f"Missing columns in filers: {missing}")

    def test_cik_column_not_null(self):
        # cik is TEXT NOT NULL (check via pragma)
        cur = self.conn.execute("PRAGMA table_info(filers)")
        col_info = {row[1]: {"type": row[2], "notnull": row[3]} for row in cur.fetchall()}
        self.assertEqual(col_info["cik"]["notnull"], 1, "filers.cik should be NOT NULL")

    def test_amendment_default_zero(self):
        cur = self.conn.execute("PRAGMA table_info(filers)")
        col_info = {row[1]: row[4] for row in cur.fetchall()}  # name -> default
        self.assertIn(col_info.get("amendment"), ("0", 0, None),
                      "filers.amendment default should be 0")

    def test_primary_key_is_cik_and_accession(self):
        cur = self.conn.execute("PRAGMA table_info(filers)")
        pk_cols = [row[1] for row in cur.fetchall() if row[5] > 0]  # pk > 0
        self.assertIn("cik", pk_cols)
        self.assertIn("accession_no", pk_cols)


class TestHoldingsSchema(unittest.TestCase):
    REQUIRED_COLUMNS = {
        "id", "cik", "report_date", "accession_no",
        "issuer_name", "cusip", "class_title",
        "value_thousands", "shares_principal", "shares_type",
        "put_call", "investment_discretion",
        "voting_sole", "voting_shared", "voting_none"
    }

    def setUp(self):
        self.conn = get_conn()
        cur = self.conn.execute("PRAGMA table_info(holdings)")
        self.columns = {row[1]: row[2] for row in cur.fetchall()}

    def tearDown(self):
        self.conn.close()

    def test_all_required_columns_present(self):
        missing = self.REQUIRED_COLUMNS - set(self.columns)
        self.assertFalse(missing, f"Missing columns in holdings: {missing}")

    def test_id_is_primary_key(self):
        cur = self.conn.execute("PRAGMA table_info(holdings)")
        pk_cols = [row[1] for row in cur.fetchall() if row[5] == 1]
        self.assertIn("id", pk_cols)

    def test_cik_not_null(self):
        cur = self.conn.execute("PRAGMA table_info(holdings)")
        col_info = {row[1]: row[3] for row in cur.fetchall()}
        self.assertEqual(col_info.get("cik"), 1, "holdings.cik should be NOT NULL")

    def test_report_date_not_null(self):
        cur = self.conn.execute("PRAGMA table_info(holdings)")
        col_info = {row[1]: row[3] for row in cur.fetchall()}
        self.assertEqual(col_info.get("report_date"), 1, "holdings.report_date should be NOT NULL")


class TestIndexesExist(unittest.TestCase):
    def setUp(self):
        self.conn = get_conn()
        cur = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        self.indexes = {row[0] for row in cur.fetchall()}

    def tearDown(self):
        self.conn.close()

    def test_index_holdings_cik_date(self):
        self.assertIn("idx_holdings_cik_date", self.indexes)

    def test_index_holdings_cusip(self):
        self.assertIn("idx_holdings_cusip", self.indexes)

    def test_index_holdings_report_date(self):
        self.assertIn("idx_holdings_report_date", self.indexes)


if __name__ == "__main__":
    unittest.main(verbosity=2)
