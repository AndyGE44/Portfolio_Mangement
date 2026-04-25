#!/usr/bin/env python3
"""
Batch-add products from a CSV file.

CSV columns:
  ticker         required  Display ticker stored in products table.
  vendor_ticker  optional  yfinance ticker if different (e.g. BRK-B for BRK.B).
                           Defaults to ticker value.
  name           optional  Company name. Auto-fetched from yfinance if blank.
  exchange       optional  Exchange code. Auto-fetched if blank.
  currency       optional  ISO currency code. Auto-fetched if blank; falls back to USD.
  asset_class    optional  stock / etf / cash / … Defaults to stock.
  backfill_start optional  YYYY-MM-DD. If set, backfills OHLCV from that date.

Usage:
    python 05_add_products_batch.py products.csv
    python 05_add_products_batch.py products.csv --dry-run
    python 05_add_products_batch.py products.csv --no-fetch
    python 05_add_products_batch.py products.csv --backfill 2020-01-01
    python 05_add_products_batch.py --sample
"""
import sys
import time
import argparse
import csv
import textwrap
import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

VENDOR_YAHOO = 3
VENDOR_YF    = 1
SOURCE_TYPE  = "eod"
CHUNK        = 2000
YF_DELAY_S   = 0.35   # pause between yfinance info calls to avoid rate-limiting

SAMPLE_CSV = textwrap.dedent("""\
    ticker,vendor_ticker,name,exchange,currency,asset_class,backfill_start
    AAPL,,,,,stock,2020-01-01
    MSFT,,,,,stock,2020-01-01
    BRK.B,BRK-B,Berkshire Hathaway Inc.,NMS,USD,stock,
    KIWI.NZ,,,,,stock,
    SPY,,,,,etf,2018-01-01
""")


# ── yfinance helpers ──────────────────────────────────────────────────────────

def yf_info(ticker: str) -> dict:
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        return {
            "name":     info.get("longName") or info.get("shortName") or "",
            "exchange": info.get("exchange") or "",
            "currency": (info.get("currency") or "").upper(),
        }
    except Exception:
        return {}


def backfill(engine, product_id: int, vendor_ticker: str, start: str):
    import yfinance as yf
    import pandas as pd

    print(f"      backfilling from {start} ...", flush=True)
    df = yf.download(vendor_ticker, start=start, auto_adjust=True,
                     progress=False, threads=False)
    if df.empty:
        print("      no data returned.")
        return 0

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    rows = []
    for idx, r in df.iterrows():
        if __import__("pandas").isna(r.get("Open")) or __import__("pandas").isna(r.get("Close")):
            continue
        rows.append({
            "pid":   product_id, "date":  idx.strftime("%Y-%m-%d"),
            "o":     float(r["Open"]),  "h": float(r["High"]),
            "l":     float(r["Low"]),   "c": float(r["Close"]),
            "v":     int(r.get("Volume", 0)),
            "stype": SOURCE_TYPE, "vid": VENDOR_YAHOO,
        })

    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        ph, params = [], {}
        for j, q in enumerate(chunk):
            ph.append(f"(:pid_{j},:dt_{j},:o_{j},:h_{j},:l_{j},:c_{j},:v_{j},:st_{j},:vid_{j})")
            params.update({
                f"pid_{j}": q["pid"],  f"dt_{j}":  q["date"],
                f"o_{j}":   q["o"],    f"h_{j}":   q["h"],
                f"l_{j}":   q["l"],    f"c_{j}":   q["c"],
                f"v_{j}":   q["v"],    f"st_{j}":  q["stype"],
                f"vid_{j}": q["vid"],
            })
        stmt = text(f"""
            INSERT INTO quotes
                (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
            VALUES {','.join(ph)}
            ON CONFLICT (product_id, trade_date, source_type, vendor_id) DO UPDATE SET
                open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, volume=EXCLUDED.volume
        """)
        with engine.begin() as conn:
            conn.execute(stmt, params)

    return len(rows)


# ── CSV reading ───────────────────────────────────────────────────────────────

def read_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = []
        for i, row in enumerate(reader, start=2):
            ticker = row.get("ticker", "").strip().upper()
            if not ticker:
                print(f"  Row {i}: missing ticker — skipped.")
                continue
            rows.append({
                "ticker":         ticker,
                "vendor_ticker":  row.get("vendor_ticker", "").strip().upper() or ticker,
                "name":           row.get("name", "").strip(),
                "exchange":       row.get("exchange", "").strip(),
                "currency":       row.get("currency", "").strip().upper(),
                "asset_class":    row.get("asset_class", "").strip() or "stock",
                "backfill_start": row.get("backfill_start", "").strip(),
            })
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Batch-add products from a CSV file.")
    parser.add_argument("csv", nargs="?", help="Path to the CSV file")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Preview what would be inserted without touching the DB")
    parser.add_argument("--no-fetch",  action="store_true",
                        help="Skip yfinance auto-lookup (use only CSV data)")
    parser.add_argument("--backfill",  metavar="START_DATE",
                        help="Override backfill start date for every product")
    parser.add_argument("--sample",    action="store_true",
                        help="Print a sample CSV to stdout and exit")
    args = parser.parse_args()

    if args.sample:
        print(SAMPLE_CSV)
        return

    if not args.csv:
        parser.error("Provide a CSV file path, or use --sample to see the format.")

    print("\n── Batch Add Products ───────────────────────────────────────────────")

    # ── Load CSV ──────────────────────────────────────────────────────────────
    try:
        rows = read_csv(args.csv)
    except FileNotFoundError:
        print(f"  File not found: {args.csv}")
        sys.exit(1)

    if not rows:
        print("  CSV has no valid rows.")
        sys.exit(1)

    print(f"  Loaded {len(rows)} row(s) from {args.csv}")

    # ── Auto-fetch missing fields ─────────────────────────────────────────────
    needs_fetch = [r for r in rows if not args.no_fetch and
                   not (r["name"] and r["exchange"] and r["currency"])]

    if needs_fetch:
        print(f"\n  Fetching info from yfinance for {len(needs_fetch)} ticker(s) ...")
        for r in needs_fetch:
            vticker = r["vendor_ticker"]
            fetched = yf_info(vticker)
            if fetched:
                r.setdefault("name",     fetched["name"])
                r.setdefault("exchange", fetched["exchange"])
                if not r["currency"]:
                    r["currency"] = fetched["currency"] or "USD"
                if not r["name"]:
                    r["name"] = fetched["name"]
                if not r["exchange"]:
                    r["exchange"] = fetched["exchange"]
            else:
                r.setdefault("currency", "USD")
            print(f"    {r['ticker']:12s}  {r['name'] or '(name not found)':40s}  {r['currency']}")
            time.sleep(YF_DELAY_S)
    else:
        for r in rows:
            r.setdefault("currency", r["currency"] or "USD")

    # Override backfill date if --backfill supplied
    if args.backfill:
        for r in rows:
            r["backfill_start"] = args.backfill

    # ── Preview table ─────────────────────────────────────────────────────────
    print(f"\n  {'TICKER':<12} {'VENDOR_TICKER':<14} {'CURRENCY':<9} {'CLASS':<7} "
          f"{'BACKFILL':<12} {'NAME'}")
    print("  " + "─" * 90)
    for r in rows:
        print(f"  {r['ticker']:<12} {r['vendor_ticker']:<14} {r['currency']:<9} "
              f"{r['asset_class']:<7} {r['backfill_start'] or '—':<12} {r['name'] or '—'}")

    if args.dry_run:
        print("\n  Dry run — nothing written.")
        return

    print()
    confirm = input(f"  Insert {len(rows)} product(s) into DB? [Y/n]: ").strip().lower()
    if confirm not in ("", "y", "yes"):
        print("  Aborted.")
        return

    # ── Insert ────────────────────────────────────────────────────────────────
    engine   = create_engine(config.DB_CONNECTION)
    inserted = skipped = errors = 0

    with engine.connect() as conn:
        valid_ccys = {r[0] for r in conn.execute(text("SELECT code FROM currencies"))}

    print()
    for r in rows:
        ticker = r["ticker"]

        if r["currency"] not in valid_ccys:
            print(f"  [{ticker}]  SKIP — currency '{r['currency']}' not in currencies table")
            errors += 1
            continue

        try:
            with engine.begin() as conn:
                existing = conn.execute(
                    text("SELECT id FROM products WHERE ticker = :t"),
                    {"t": ticker},
                ).fetchone()

                if existing:
                    product_id = existing[0]
                    # Still upsert vendor mappings in case they're missing
                    action = "exists"
                else:
                    row = conn.execute(
                        text("""
                            INSERT INTO products
                                (ticker, name, base_currency, exchange, asset_class, is_active)
                            VALUES (:ticker, :name, :currency, :exchange, :asset_class, true)
                            RETURNING id
                        """),
                        {
                            "ticker":      ticker,
                            "name":        r["name"] or None,
                            "currency":    r["currency"],
                            "exchange":    r["exchange"] or None,
                            "asset_class": r["asset_class"],
                        },
                    ).fetchone()
                    product_id = row[0]
                    action = "inserted"
                    inserted += 1

                for vid in {VENDOR_YAHOO, VENDOR_YF}:
                    conn.execute(
                        text("""
                            INSERT INTO vendor_mappings (product_id, vendor_id, vendor_ticker)
                            VALUES (:pid, :vid, :vt)
                            ON CONFLICT (product_id, vendor_id)
                            DO UPDATE SET vendor_ticker = EXCLUDED.vendor_ticker
                        """),
                        {"pid": product_id, "vid": vid, "vt": r["vendor_ticker"]},
                    )

            tag = f"(id={product_id})"
            if action == "exists":
                print(f"  [{ticker}]  already exists {tag} — vendor mapping refreshed")
                skipped += 1
            else:
                print(f"  [{ticker}]  {action} {tag}")

        except SQLAlchemyError as e:
            print(f"  [{ticker}]  ERROR — {e}")
            errors += 1
            continue

        # ── Per-row backfill ──────────────────────────────────────────────────
        if r["backfill_start"] and action != "exists":
            try:
                n = backfill(engine, product_id, r["vendor_ticker"], r["backfill_start"])
                print(f"      {n:,} quotes backfilled")
            except Exception as e:
                print(f"      backfill failed: {e}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"""
  ── Summary ──────────────────────────────────
    Inserted : {inserted}
    Skipped  : {skipped}  (already existed)
    Errors   : {errors}
  ─────────────────────────────────────────────
""")


if __name__ == "__main__":
    main()
