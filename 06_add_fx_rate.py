#!/usr/bin/env python3
"""
!!!Need to be more robust for all kind of input.!!!

Add USD ↔ HKD FX rates to the database.
Always inserts both directions: USD→HKD and HKD→USD (= 1 / rate).

Three modes:
  Manual  (default)  — enter one date and rate; or leave blank to enter multiple
  Fetch   (--fetch)  — auto-download daily closing rates from yfinance for a date range
  CSV     (--csv)    — import from a CSV file (columns: date, usd_hkd)

Usage:
    python 06_add_fx_rate.py
    python 06_add_fx_rate.py --date 2025-01-15 --rate 7.7850
    python 06_add_fx_rate.py --fetch 2024-01-01 2024-12-31
    python 06_add_fx_rate.py --fetch 2020-01-01 2025-12-31 --dry-run
    python 06_add_fx_rate.py --csv fx_rates.csv
    python 06_add_fx_rate.py --sample
"""
import sys
import argparse
import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

VENDOR_MANUAL       = 4
VENDOR_YAHOO        = 3
FROM_CCY, TO_CCY    = "USD", "HKD"     # canonical direction; inverse computed automatically
YF_TICKER           = "USDHKD=X"       # Yahoo Finance symbol for 1 USD in HKD

SAMPLE_CSV = "date,usd_hkd\n2024-01-02,7.8094\n2024-01-03,7.8121\n2024-01-04,7.8050\n"


# ── DB helpers ────────────────────────────────────────────────────────────────

def upsert_pair(conn, date_str: str, usd_hkd: float, vendor_id: int):
    """Insert (or overwrite) the USD→HKD and HKD→USD rows for one date."""
    hkd_usd = round(1.0 / usd_hkd, 10)
    conn.execute(
        text("""
            INSERT INTO fx_rates (from_currency, to_currency, rate_date, rate, vendor_id)
            VALUES
                (:fc1, :tc1, :d, :r1, :vid),
                (:fc2, :tc2, :d, :r2, :vid)
            ON CONFLICT (from_currency, to_currency, rate_date) DO UPDATE
                SET rate = EXCLUDED.rate, vendor_id = EXCLUDED.vendor_id,
                    updated_at = now()
        """),
        {
            "fc1": FROM_CCY, "tc1": TO_CCY,   "r1": round(usd_hkd, 10),
            "fc2": TO_CCY,   "tc2": FROM_CCY, "r2": hkd_usd,
            "d":  date_str,  "vid": vendor_id,
        },
    )


def existing_dates(conn) -> set:
    rows = conn.execute(
        text("SELECT rate_date FROM fx_rates WHERE from_currency='USD' AND to_currency='HKD'")
    ).fetchall()
    return {str(r[0]) for r in rows}


# ── yfinance fetch ────────────────────────────────────────────────────────────

def fetch_from_yfinance(start: str, end: str) -> list[tuple[str, float]]:
    """Download daily USDHKD closing rates from yfinance. Returns [(date_str, rate), ...]."""
    import yfinance as yf
    import pandas as pd

    print(f"  Downloading {YF_TICKER} from {start} to {end} ...", flush=True)
    df = yf.download(YF_TICKER, start=start, end=end,
                     auto_adjust=False, progress=False, threads=False)
    if df.empty:
        print("  No data returned.")
        return []

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    result = []
    for idx, row in df.iterrows():
        if pd.isna(row.get("Close")):
            continue
        result.append((idx.strftime("%Y-%m-%d"), float(row["Close"])))
    return result


# ── CSV read ──────────────────────────────────────────────────────────────────

def read_csv(path: str) -> list[tuple[str, float]]:
    import csv
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):
            date_str = row.get("date", "").strip()
            rate_str = row.get("usd_hkd", "").strip()
            if not date_str or not rate_str:
                print(f"  Row {i}: missing date or usd_hkd — skipped.")
                continue
            try:
                rows.append((date_str, float(rate_str)))
            except ValueError:
                print(f"  Row {i}: invalid rate '{rate_str}' — skipped.")
    return rows


# ── Bulk insert helper ────────────────────────────────────────────────────────

def insert_rates(engine, pairs: list[tuple[str, float]], vendor_id: int,
                 dry_run: bool = False, skip_existing: bool = False):
    """
    Insert a list of (date_str, usd_hkd_rate) pairs.
    Prints a summary at the end.
    """
    if not pairs:
        print("  Nothing to insert.")
        return

    with engine.connect() as conn:
        already = existing_dates(conn) if skip_existing else set()

    skipped = [p for p in pairs if p[0] in already]
    to_insert = [p for p in pairs if p[0] not in already] if skip_existing else pairs

    if skip_existing and skipped:
        print(f"  Skipping {len(skipped)} date(s) already in DB.")

    if not to_insert:
        print("  No new dates to insert.")
        return

    print(f"\n  {'DATE':<14} {'USD→HKD':>10}  {'HKD→USD':>12}")
    print("  " + "─" * 42)
    for date_str, rate in to_insert:
        print(f"  {date_str:<14} {rate:>10.4f}  {1/rate:>12.8f}")

    if dry_run:
        print(f"\n  Dry run — {len(to_insert)} pair(s) would be inserted. Nothing written.")
        return

    try:
        with engine.begin() as conn:
            for date_str, rate in to_insert:
                upsert_pair(conn, date_str, rate, vendor_id)
        print(f"\n  Done — {len(to_insert)} date(s) upserted ({len(to_insert) * 2} rows total).")
    except SQLAlchemyError as e:
        print(f"  DB error: {e}")


# ── Input validation ──────────────────────────────────────────────────────────

import re as _re
_DATE_RE = _re.compile(r"^\d{4}-\d{2}-\d{2}$")

def parse_date(s: str, label: str = "date") -> str:
    s = s.strip()
    if not _DATE_RE.match(s):
        print(f"  Invalid {label} '{s}' — expected YYYY-MM-DD.")
        sys.exit(1)
    return s

def parse_rate(s: str) -> float:
    try:
        r = float(s.strip())
        if r <= 0:
            raise ValueError
        return r
    except ValueError:
        print(f"  Invalid rate '{s}' — must be a positive number.")
        sys.exit(1)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Add USD↔HKD FX rates.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "examples:\n"
            "  python 06_add_fx_rate.py                               # interactive\n"
            "  python 06_add_fx_rate.py --date 2025-01-15 --rate 7.785\n"
            "  python 06_add_fx_rate.py --fetch 2020-01-01 2026-04-24\n"
            "  python 06_add_fx_rate.py --csv fx_rates.csv\n"
        ),
    )

    parser.add_argument("--date",  metavar="YYYY-MM-DD", help="Single date (manual mode)")
    parser.add_argument("--rate",  metavar="RATE",       help="USD→HKD rate for --date", type=float)
    parser.add_argument("--fetch", nargs=2, metavar=("START_DATE", "END_DATE"),
                        help="Download rates from yfinance for a date range")
    parser.add_argument("--csv",   metavar="FILE",
                        help="Import from CSV file (columns: date, usd_hkd)")
    parser.add_argument("--dry-run",       action="store_true",
                        help="Preview without writing to the DB")
    parser.add_argument("--skip-existing", action="store_true",
                        help="Skip dates already present in fx_rates")
    parser.add_argument("--sample",        action="store_true",
                        help="Print a sample CSV to stdout and exit")

    args = parser.parse_args()

    if args.sample:
        print(SAMPLE_CSV)
        return

    engine = create_engine(config.DB_CONNECTION)

    # ── --fetch ───────────────────────────────────────────────────────────────
    if args.fetch:
        start = parse_date(args.fetch[0], "start date")
        end   = parse_date(args.fetch[1], "end date")
        if start > end:
            print("  Start date must be on or before end date.")
            sys.exit(1)
        pairs = fetch_from_yfinance(start, end)
        print(f"  Fetched {len(pairs)} trading day(s).")
        insert_rates(engine, pairs, VENDOR_YAHOO,
                     dry_run=args.dry_run, skip_existing=args.skip_existing)
        return

    # ── --csv ─────────────────────────────────────────────────────────────────
    if args.csv:
        pairs = read_csv(args.csv)
        print(f"  Read {len(pairs)} row(s) from {args.csv}.")
        insert_rates(engine, pairs, VENDOR_MANUAL,
                     dry_run=args.dry_run, skip_existing=args.skip_existing)
        return

    # ── --date / --rate (non-interactive single entry) ────────────────────────
    print("\n── Add FX Rate (USD ↔ HKD) ─────────────────────────────────────────")

    if args.date or args.rate:
        if not args.date or not args.rate:
            print("  --date and --rate must be used together.")
            sys.exit(1)
        date_str = parse_date(args.date)
        rate     = parse_rate(str(args.rate))
        insert_rates(engine, [(date_str, rate)], VENDOR_MANUAL, dry_run=args.dry_run)
        return

    # ── Fully interactive ─────────────────────────────────────────────────────
    print("  Enter one rate per line. Leave date blank to finish.\n")
    pairs = []
    while True:
        date_str = input("  Date (YYYY-MM-DD) or Enter to finish: ").strip()
        if not date_str:
            break
        if not _DATE_RE.match(date_str):
            print("    Expected YYYY-MM-DD — try again.")
            continue
        rate_str = input(f"  USD→HKD rate on {date_str}: ").strip()
        try:
            rate = float(rate_str)
            if rate <= 0:
                raise ValueError
        except ValueError:
            print("    Must be a positive number — try again.")
            continue
        pairs.append((date_str, rate))
        print(f"    → HKD→USD = {1/rate:.8f}  ✓")

    if not pairs:
        print("  Nothing entered.")
        return

    insert_rates(engine, pairs, VENDOR_MANUAL, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
