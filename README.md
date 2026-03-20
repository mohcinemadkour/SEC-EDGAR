# SEC EDGAR 13F Holdings Dataset Pipeline

Builds a production-grade **manager–security–quarter panel** from SEC EDGAR structured 13F-HR filings (2010–present).

---

## Schema

### `filers` table
| Column | Type | Description |
|--------|------|-------------|
| `cik` | TEXT | SEC Central Index Key (10-digit padded) |
| `manager_name` | TEXT | Institutional manager name |
| `filing_date` | TEXT | Date filing submitted to SEC |
| `report_date` | TEXT | Quarter-end date (e.g. 2023-09-30) |
| `accession_no` | TEXT | EDGAR accession number |
| `form_type` | TEXT | 13F-HR or 13F-HR/A (amendment) |
| `amendment` | INTEGER | 1 if amendment, 0 if original |

### `holdings` table
| Column | Type | Description |
|--------|------|-------------|
| `cik` | TEXT | Manager CIK |
| `report_date` | TEXT | Quarter-end date |
| `accession_no` | TEXT | Filing accession number |
| `issuer_name` | TEXT | Name of issuer |
| `cusip` | TEXT | 9-character CUSIP |
| `class_title` | TEXT | Class of security (e.g. COM, NOTE) |
| `value_thousands` | INTEGER | Market value in thousands of USD |
| `shares_principal` | INTEGER | Shares or principal amount |
| `shares_type` | TEXT | SH (shares) or PRN (principal) |
| `put_call` | TEXT | Put, Call, or NULL |
| `investment_discretion` | TEXT | SOLE, DFND, OTR |
| `voting_sole` | INTEGER | Shares with sole voting authority |
| `voting_shared` | INTEGER | Shares with shared voting authority |
| `voting_none` | INTEGER | Shares with no voting authority |

---

## Installation

```bash
pip install requests  # optional — pipeline uses only stdlib
```

Python 3.9+ required. No external dependencies (uses `urllib` and `xml.etree`).

---

## Usage

### Ingest specific managers
```bash
# Berkshire Hathaway + BlackRock, all filings from 2020
python pipeline.py --ciks 0001067983 0001350694 --start 2020-01-01

# Test run: 2 filings per manager
python pipeline.py --ciks 0001067983 --start 2023-01-01 --limit 2
```

### Ingest top managers (built-in list)
```bash
# Top 10 managers from 2015
python pipeline.py --top-managers 10 --start 2015-01-01

# Full 2010-present for top 50
python pipeline.py --top-managers 50 --start 2010-01-01
```

### Export to CSV
```bash
python pipeline.py --ciks 0001067983 --start 2023-01-01 --export-csv
# → filers.csv, holdings.csv
```

---

## Queries

```bash
# Dataset summary
python query.py summary

# Top 25 holdings for Berkshire Q3 2023
python query.py top-holdings --cik 0001067983 --quarter 2023-09-30

# All managers holding Apple (CUSIP: 037833100)
python query.py position-history --cusip 037833100

# Consensus buys in Q4 2023 (≥5 managers adding)
python query.py consensus-buys --quarter 2023-12-31 --min-managers 5

# Full manager-quarter panel exported to CSV
python query.py manager-panel --cik 0001067983 --export-csv berkshire_panel.csv

# Portfolio overlap matrix
python query.py overlap --quarter 2023-09-30 --top-n 8
```

---

## Architecture

```
SEC EDGAR API
  ├── /submissions/CIK{n}.json         → filing metadata
  └── /Archives/edgar/data/{cik}/...
        └── {accession}-index.json     → document index
              └── infotable.xml        → holdings XML (parsed)

pipeline.py
  ├── get_filings_for_cik()  → discovers all 13F-HR accessions
  ├── get_13f_xml_url()      → resolves information table document
  ├── parse_13f_xml()        → extracts infoTable entries
  └── run_pipeline()         → orchestrates → SQLite (edgar_13f.db)

query.py
  ├── summary()              → dataset stats
  ├── top_holdings()         → top positions per manager/quarter
  ├── position_history()     → CUSIP time series
  ├── consensus_buys()       → crowd-sourced momentum signal
  ├── manager_panel()        → full panel export
  └── overlap_matrix()       → pairwise portfolio overlap
```

---

## Notes

- **Rate limiting**: Pipeline sleeps 120ms between requests to stay within SEC's 10 req/s limit.
- **Amendments**: 13F-HR/A filings are ingested and flagged (`amendment=1`). For analysis, typically keep only the latest filing per CIK+quarter.
- **XML namespaces**: EDGAR 13F XML uses multiple namespace variants across years. The parser handles `ns1`, `ns2`, and `n1` namespaces automatically.
- **Full 2010–present run**: ~50 top managers × ~55 quarters × avg 500 holdings ≈ 1.4M rows. Expect ~3–5 hours with rate limiting.

---

## Data Sources

All data sourced from **SEC EDGAR** public API:
- `https://data.sec.gov/submissions/`
- `https://data.sec.gov/Archives/edgar/`

No API key required. Free and public.
