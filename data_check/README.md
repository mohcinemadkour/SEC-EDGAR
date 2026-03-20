# Phase 1 Data Validation

Tests for validating the SEC EDGAR 13F pipeline output (`edgar_13f.db`).

## Structure

| File | What it tests | Network? |
|---|---|---|
| `test_schema.py` | DB tables, columns, types, PKs, indexes | No |
| `test_data_quality.py` | Content rules: dates, CUSIP format, FK integrity, allowed values | No |
| `test_pipeline_units.py` | `parse_13f_xml`, `get_13f_xml_url`, namespace handling, edge cases | No |
| `test_live_edgar.py` | Real EDGAR API calls, end-to-end fetch→parse on a known filing | Yes |
| `run_all.py` | Master runner — runs all suites and prints a report | — |

## Usage

```bash
# Run all offline tests (schema + quality + unit) — recommended default
python data_check/run_all.py

# Include live EDGAR network tests
python data_check/run_all.py --live

# Run only one suite
python data_check/run_all.py --only schema
python data_check/run_all.py --only quality
python data_check/run_all.py --only unit
python data_check/run_all.py --only live
```

## Prerequisites

- The pipeline must have run at least once: `python pipeline.py --ciks 0001067983 --start 2023-01-01 --limit 2`
- `edgar_13f.db` must exist in the project root

## What Phase 1 validates

### Schema checks
- Both `filers` and `holdings` tables exist
- All required columns are present with correct nullability
- Primary keys and indexes are in place

### Data quality checks
- No NULL values in required fields (CIK, accession_no, report_date)
- CIK is zero-padded to 10 digits
- Accession numbers match format `XXXXXXXXXX-YY-ZZZZZZ`
- Dates are `YYYY-MM-DD` format
- `report_date` is always a valid quarter-end (03-31, 06-30, 09-30, 12-31)
- `filing_date` is not before `report_date`
- `form_type` is `13F-HR` or `13F-HR/A`
- `amendment` flag matches form_type
- CUSIP is exactly 9 alphanumeric characters
- `value_thousands` and `shares_principal` are non-negative
- `shares_type` is `SH` or `PRN`
- `investment_discretion` is `SOLE`, `DFND`, or `OTR`
- No duplicate `(cik, accession_no)` in filers
- All holdings rows reference a valid filer (referential integrity)

### Unit tests (no network)
- XML parser handles all 4 known SEC 13F namespace variants
- Parser returns correct field values
- Edge cases: empty bytes, malformed XML, BOM prefix, multiple entries
- `get_13f_xml_url` picks raw XML over XSLT copies
- `get_13f_xml_url` uses `www.sec.gov` not `data.sec.gov`

### Live integration tests (network)
- EDGAR submissions API is reachable
- `get_filings_for_cik` returns valid 13F filings for Berkshire
- `get_13f_xml_url` resolves a known accession number
- End-to-end parse of a real filing produces valid holdings rows
