#!/usr/bin/env python3
"""
Add a new product (stock, ETF, etc.) to the database interactively.
Looks up company info via yfinance automatically, then lets you confirm or override.
Inserts into products + vendor_mappings, and optionally backfills historical prices.

Usage:
    python 03_add_product.py
    python 03_add_product.py --ticker KIWI.NZ
    python 03_add_product.py --ticker AAPL --backfill 2015-01-01
"""
import sys
import argparse
import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

VENDOR_YAHOO = 3   # yahoo_finance — used by daily price sync
VENDOR_YF    = 1   # yfinance library
SOURCE_TYPE  = "eod"
CHUNK        = 2000


def ask(label, default=None, *, required=True):
    hint = f" [{default}]" if default is not None else ""
    while True:
        val = input(f"  {label}{hint}: ").strip()
        if val:
            return val
        if default is not None:
            return str(default)
        if not required:
            return ""
        print("    (required — please enter a value)")


def yf_info(ticker):
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


def backfill(engine, product_id, vendor_ticker, start):
    import yfinance as yf
    import pandas as pd

    print(f"  Downloading history from {start} ...", flush=True)
    df = yf.download(vendor_ticker, start=start, auto_adjust=True,
                     progress=False, threads=False)
    if df.empty:
        print("  No data returned from yfinance.")
        return

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    rows = []
    for idx, r in df.iterrows():
        if pd.isna(r.get("Open")) or pd.isna(r.get("Close")):
            continue
        rows.append({
            "pid":   product_id,
            "date":  idx.strftime("%Y-%m-%d"),
            "o":     float(r["Open"]),
            "h":     float(r["High"]),
            "l":     float(r["Low"]),
            "c":     float(r["Close"]),
            "v":     int(r.get("Volume", 0)),
            "stype": SOURCE_TYPE,
            "vid":   VENDOR_YAHOO,
        })

    if not rows:
        print("  No valid rows after filtering NaNs.")
        return

    print(f"  Inserting {len(rows)} rows ...", flush=True)
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i : i + CHUNK]
        ph, params = [], {}
        for j, q in enumerate(chunk):
            ph.append(
                f"(:pid_{j},:dt_{j},:o_{j},:h_{j},:l_{j},:c_{j},:v_{j},:st_{j},:vid_{j})"
            )
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

    print(f"  Done — {len(rows)} quotes inserted.")


def main():
    parser = argparse.ArgumentParser(description="Add a product to the database.")
    parser.add_argument("--ticker",   help="Display ticker, e.g. AAPL or KIWI.NZ")
    parser.add_argument("--backfill", metavar="START_DATE",
                        help="Also backfill prices starting from this date (YYYY-MM-DD)")
    args = parser.parse_args()

    print("\n── Add New Product ──────────────────────────────────────────────────")

    # ── Display ticker ────────────────────────────────────────────────────────
    display_ticker = (args.ticker or "").upper() or ask("Display ticker (e.g. AAPL, KIWI.NZ)").upper()

    # ── Auto-fetch from yfinance ──────────────────────────────────────────────
    print(f"\n  Fetching info for '{display_ticker}' from yfinance ...")
    fetched = yf_info(display_ticker)
    if fetched.get("name"):
        print(f"  Found: {fetched['name']}  |  exchange={fetched['exchange']}  |  currency={fetched['currency']}")
    else:
        print("  Nothing found automatically — you will fill fields manually.")

    # ── Vendor ticker (may differ from display ticker) ────────────────────────
    # e.g. display "BRK.B"  →  yfinance vendor ticker "BRK-B"
    vendor_ticker = ask(
        "Vendor ticker for yfinance (press Enter if same as display ticker)",
        default=display_ticker,
    ).upper()

    # Re-fetch if user gave a different vendor ticker and auto-fetch found nothing
    if vendor_ticker != display_ticker and not fetched.get("name"):
        print(f"  Re-fetching with '{vendor_ticker}' ...")
        fetched = yf_info(vendor_ticker)
        if fetched.get("name"):
            print(f"  Found: {fetched['name']}  |  exchange={fetched['exchange']}  |  currency={fetched['currency']}")

    # ── Show valid currencies from DB ─────────────────────────────────────────
    engine = create_engine(config.DB_CONNECTION)
    with engine.connect() as conn:
        valid_ccys = [r[0] for r in conn.execute(text("SELECT code FROM currencies ORDER BY code"))]
    print(f"\n  Valid currencies in DB: {', '.join(valid_ccys)}")
    print("  (If you need a new currency, add it to the currencies table first.)")

    # ── Confirm / override fields ─────────────────────────────────────────────
    name        = ask("Company / product name",        default=fetched.get("name") or None)
    exchange    = ask("Exchange",                      default=fetched.get("exchange") or None, required=False)
    currency    = ask("Base currency",                 default=fetched.get("currency") or "USD").upper()
    asset_class = ask("Asset class (stock/cash/etf)", default="stock")

    print(f"""
  ┌─ Summary ──────────────────────────────────────────┐
  │  Display ticker : {display_ticker}
  │  Vendor ticker  : {vendor_ticker}
  │  Name           : {name}
  │  Exchange       : {exchange or '(blank)'}
  │  Currency       : {currency}
  │  Asset class    : {asset_class}
  └────────────────────────────────────────────────────┘""")

    if input("  Proceed? [Y/n]: ").strip().lower() not in ("", "y", "yes"):
        print("  Aborted.")
        return

    # ── Insert product + vendor mappings ──────────────────────────────────────
    try:
        with engine.begin() as conn:
            existing = conn.execute(
                text("SELECT id FROM products WHERE ticker = :t"),
                {"t": display_ticker},
            ).fetchone()

            if existing:
                product_id = existing[0]
                print(f"\n  '{display_ticker}' already exists (id={product_id}).")
                if input("  Update vendor mapping for existing product? [y/N]: ").strip().lower() \
                        not in ("y", "yes"):
                    print("  Aborted.")
                    return
            else:
                row = conn.execute(
                    text("""
                        INSERT INTO products (ticker, name, base_currency, exchange, asset_class, is_active)
                        VALUES (:ticker, :name, :currency, :exchange, :asset_class, true)
                        RETURNING id
                    """),
                    {
                        "ticker":      display_ticker,
                        "name":        name,
                        "currency":    currency,
                        "exchange":    exchange or None,
                        "asset_class": asset_class,
                    },
                ).fetchone()
                product_id = row[0]
                print(f"\n  Product inserted (id={product_id})")

            for vid in {VENDOR_YAHOO, VENDOR_YF}:
                conn.execute(
                    text("""
                        INSERT INTO vendor_mappings (product_id, vendor_id, vendor_ticker)
                        VALUES (:pid, :vid, :vt)
                        ON CONFLICT (product_id, vendor_id)
                        DO UPDATE SET vendor_ticker = EXCLUDED.vendor_ticker
                    """),
                    {"pid": product_id, "vid": vid, "vt": vendor_ticker},
                )
            print(f"  Vendor mappings set for yahoo_finance + yfinance  →  '{vendor_ticker}'")

    except SQLAlchemyError as e:
        print(f"\n  DB error: {e}")
        return

    # ── Optional price backfill ───────────────────────────────────────────────
    if args.backfill:
        backfill(engine, product_id, vendor_ticker, args.backfill)
    elif input("\n  Backfill historical prices from yfinance? [y/N]: ").strip().lower() in ("", "y", "yes"):
        start = ask("Start date", default="2020-01-01")
        backfill(engine, product_id, vendor_ticker, start)

    print("\n  All done.\n")


if __name__ == "__main__":
    main()
