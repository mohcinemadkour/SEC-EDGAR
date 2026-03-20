"""
run_all.py
Master test runner for Phase 1 data validation.

Usage:
    python data_check/run_all.py              # schema + data quality + unit tests
    python data_check/run_all.py --live       # also run live EDGAR network tests
    python data_check/run_all.py --only unit  # run only unit tests
    python data_check/run_all.py --only live  # run only live tests

Exit code: 0 = all passed, 1 = failures exist
"""

import argparse
import io
import sys
import time
import unittest
from datetime import datetime
from pathlib import Path

# Ensure project root is on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

SUITES = {
    "schema":  "test_schema",
    "quality": "test_data_quality",
    "unit":    "test_pipeline_units",
    "live":    "test_live_edgar",
}

OFFLINE_SUITES = ["schema", "quality", "unit"]
ALL_SUITES     = list(SUITES.keys())


def load_suite(name: str) -> unittest.TestSuite:
    loader = unittest.TestLoader()
    # Load from the data_check directory
    sys.path.insert(0, str(Path(__file__).parent))
    suite = loader.loadTestsFromName(SUITES[name])
    return suite


def run_suites(names: list[str], report_path: Path = None) -> bool:
    """Run named suites, print results, return True if all passed."""
    start = time.time()
    total_suite = unittest.TestSuite()
    for name in names:
        try:
            total_suite.addTests(load_suite(name))
        except Exception as e:
            print(f"  [ERROR] Could not load suite '{name}': {e}")
            return False

    header = (
        "=" * 70 + "\n"
        f"  SEC EDGAR Phase 1 — Data Validation\n"
        f"  Run date : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
        f"  Suites   : {', '.join(names)}\n"
        + "=" * 70
    )
    print(header)

    # Capture output so we can tee to file
    buf = io.StringIO()
    tee = _TeeStream(sys.stdout, buf)

    runner = unittest.TextTestRunner(
        verbosity=2,
        stream=tee,
        descriptions=True,
        failfast=False,
    )
    result = runner.run(total_suite)

    elapsed = time.time() - start
    summary_lines = ["\n" + "=" * 70]
    summary_lines.append(f"  Ran {result.testsRun} tests in {elapsed:.1f}s")
    if result.wasSuccessful():
        summary_lines.append("  ALL TESTS PASSED")
    else:
        if result.failures:
            summary_lines.append(f"  FAILURES : {len(result.failures)}")
        if result.errors:
            summary_lines.append(f"  ERRORS   : {len(result.errors)}")
        if result.skipped:
            summary_lines.append(f"  SKIPPED  : {len(result.skipped)}")
    summary_lines.append("=" * 70)
    summary = "\n".join(summary_lines)
    print(summary)

    if report_path:
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(header + "\n")
            f.write(buf.getvalue())
            f.write(summary + "\n")
        print(f"\n  Report saved → {report_path}")

    return result.wasSuccessful()


class _TeeStream:
    """Writes to two streams simultaneously."""
    def __init__(self, *streams):
        self._streams = streams
    def write(self, data):
        for s in self._streams:
            s.write(data)
    def writeln(self, data=""):
        self.write(data + "\n")
    def flush(self):
        for s in self._streams:
            s.flush()


def main():
    parser = argparse.ArgumentParser(
        description="Run Phase 1 data validation checks"
    )
    parser.add_argument(
        "--live", action="store_true",
        help="Include live EDGAR network tests (makes real HTTP calls)"
    )
    parser.add_argument(
        "--only", choices=list(SUITES.keys()),
        help="Run only a specific suite"
    )
    parser.add_argument(
        "--report", metavar="FILE",
        nargs="?",
        const="auto",   # --report with no value → auto-named file
        help="Save results to a file. Omit value for auto-named report in data_check/reports/"
    )
    args = parser.parse_args()

    if args.only:
        suites_to_run = [args.only]
    elif args.live:
        suites_to_run = ALL_SUITES
    else:
        suites_to_run = OFFLINE_SUITES

    report_path = None
    if args.report:
        if args.report == "auto":
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = Path(__file__).parent / "reports" / f"report_{ts}.txt"
        else:
            report_path = Path(args.report)

    success = run_suites(suites_to_run, report_path=report_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
