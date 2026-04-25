#!/usr/bin/env python3
"""
One-off rebuild of `quotes` history with auto_adjust=True (split/dividend-adjusted prices).

Why: the daily sync used to store *raw* prices (auto_adjust=False). When a stock
splits, replaying transactions gives us the post-split share count, but the old
raw prices are pre-split — which double-counts NAV on the split date. Switching
to adjusted prices makes split-date NAV invariant.

This script DELETES every row in `quotes` for vendor=yahoo_finance with
source_type='eod' (within --start..--end), then re-downloads and re-inserts.

Usage:
    python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-25
    python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-25 --asset-class stock
    python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-25 --tickers AAPL,MSFT
    python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-25 --dry-run

Run this ONCE after switching auto_adjust=True. After that, the daily cron
keeps everything in adjusted-price space.
"""
import argparse
import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import yfinance as yf
import pandas as pd
import config

VENDOR_NAME  = "yahoo_finance"
SOURCE_TYPE  = "eod"
CHUNK        = 5000
BATCH_DELAY  = 0.5    # seconds between yfinance batches
BATCH_SIZE   = 50     # tickers per yf.download call


def load_targets(engine, asset_class: str | None, ticker_filter: list[str] | None):
    sql = """
        SELECT p.id, p.ticker, p.asset_class, vm.vendor_ticker
        FROM   products p
        JOIN   vendor_mappings vm ON vm.product_id = p.id
        JOIN   vendors v          ON v.id = vm.vendor_id
        WHERE  v.name = :vname
          AND  p.is_active = TRUE
          AND  p.asset_class <> 'cash'
    """
    params = {"vname": VENDOR_NAME}
    if asset_class:
        sql += " AND p.asset_class = :ac"
        params["ac"] = asset_class
    if ticker_filter:
        sql += " AND p.ticker = ANY(:tks)"
        params["tks"] = ticker_filter
    sql += " ORDER BY p.ticker"
    with engine.connect() as conn:
        return conn.execute(text(sql), params).fetchall()


def delete_range(engine, vendor_id: int, product_ids: list[int],
                 start: str, end: str, dry_run: bool):
    if dry_run:
        with engine.connect() as conn:
            n = conn.execute(text("""
                SELECT COUNT(*) FROM quotes
                WHERE vendor_id = :vid AND source_type = :st
                  AND product_id = ANY(:pids)
                  AND trade_date BETWEEN :s AND :e
            """), {"vid": vendor_id, "st": SOURCE_TYPE,
                   "pids": product_ids, "s": start, "e": end}).scalar()
        print(f"  [dry-run] would delete {n:,} existing quote rows.")
        return
    with engine.begin() as conn:
        deleted = conn.execute(text("""
            DELETE FROM quotes
            WHERE vendor_id = :vid AND source_type = :st
              AND product_id = ANY(:pids)
              AND trade_date BETWEEN :s AND :e
        """), {"vid": vendor_id, "st": SOURCE_TYPE,
               "pids": product_ids, "s": start, "e": end}).rowcount
    print(f"  Deleted {deleted:,} existing rows.")


def download_batch(vtickers: list[str], start: str, end: str) -> pd.DataFrame:
    df = yf.download(
        vtickers, start=start, end=end,
        auto_adjust=True, threads=True, progress=False, ignore_tz=True,
    )
    if df.empty:
        return df
    # yf.download returns MultiIndex when len(tickers)>1, flat columns otherwise
    if len(vtickers) == 1 and not isinstance(df.columns, pd.MultiIndex):
        df.columns = pd.MultiIndex.from_product([df.columns, [vtickers[0]]])
    return df


def insert_chunk(engine, vendor_id: int, rows: list[dict]):
    if not rows:
        return 0
    inserted = 0
    for i in range(0, len(rows), CHUNK):
        chunk = rows[i:i + CHUNK]
        ph, params = [], {}
        for j, q in enumerate(chunk):
            ph.append(f"(:pid_{j},:dt_{j},:o_{j},:h_{j},:l_{j},:c_{j},:v_{j},:st_{j},:vid_{j})")
            params.update({
                f"pid_{j}": q["pid"],  f"dt_{j}":  q["date"],
                f"o_{j}":   q["o"],    f"h_{j}":   q["h"],
                f"l_{j}":   q["l"],    f"c_{j}":   q["c"],
                f"v_{j}":   q["v"],    f"st_{j}":  SOURCE_TYPE,
                f"vid_{j}": vendor_id,
            })
        stmt = text(f"""
            INSERT INTO quotes
                (product_id, trade_date, open, high, low, close, volume, source_type, vendor_id)
            VALUES {','.join(ph)}
            ON CONFLICT (product_id, trade_date, source_type, vendor_id) DO UPDATE SET
                open = EXCLUDED.open, high = EXCLUDED.high,
                low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume,
                updated_at = NOW()
        """)
        with engine.begin() as conn:
            conn.execute(stmt, params)
        inserted += len(chunk)
    return inserted


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start",       required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--end",         required=True, help="YYYY-MM-DD inclusive")
    ap.add_argument("--asset-class", default=None,
                    help="Limit to products of this asset_class (e.g. stock, etf)")
    ap.add_argument("--tickers",     default=None,
                    help="Comma-separated display tickers to limit (e.g. AAPL,MSFT)")
    ap.add_argument("--batch-size",  type=int, default=BATCH_SIZE)
    ap.add_argument("--dry-run",     action="store_true")
    args = ap.parse_args()

    # Validate dates
    try:
        datetime.strptime(args.start, "%Y-%m-%d")
        datetime.strptime(args.end,   "%Y-%m-%d")
    except ValueError:
        print("  --start and --end must be YYYY-MM-DD")
        sys.exit(1)

    ticker_filter = [t.strip().upper() for t in args.tickers.split(",")] if args.tickers else None
    engine        = create_engine(config.DB_CONNECTION)

    with engine.connect() as conn:
        vendor_id = conn.execute(
            text("SELECT id FROM vendors WHERE name = :n"), {"n": VENDOR_NAME}
        ).scalar()
        if not vendor_id:
            print(f"  Vendor '{VENDOR_NAME}' not found.")
            sys.exit(1)

    targets = load_targets(engine, args.asset_class, ticker_filter)
    if not targets:
        print("  No matching products.")
        return

    print(f"  Re-syncing {len(targets)} product(s) from {args.start} → {args.end}")
    print(f"  auto_adjust=True, batch_size={args.batch_size}, dry_run={args.dry_run}")
    print()

    # Map vendor_ticker → product_id  (yfinance returns by vendor ticker)
    vt_to_pid = {row.vendor_ticker: row.id for row in targets}
    pids      = [row.id for row in targets]

    # Delete existing rows in range first (idempotency)
    print("  ── Delete pass ──")
    delete_range(engine, vendor_id, pids, args.start, args.end, args.dry_run)
    print()

    # Download & insert in batches
    print("  ── Download + insert pass ──")
    vtickers_all = list(vt_to_pid.keys())
    total_rows   = 0
    failed_batch = []

    for bstart in range(0, len(vtickers_all), args.batch_size):
        batch = vtickers_all[bstart:bstart + args.batch_size]
        idx   = bstart // args.batch_size + 1
        nbatch = (len(vtickers_all) + args.batch_size - 1) // args.batch_size
        print(f"    [{idx}/{nbatch}] downloading {len(batch)} ticker(s) ...", flush=True)

        try:
            df = download_batch(batch, args.start, args.end)
        except Exception as e:
            print(f"      download failed: {e}")
            failed_batch.extend(batch)
            continue

        if df.empty:
            print("      empty result — skipped.")
            continue

        # Stack: columns are MultiIndex (field, ticker). Build flat row dicts.
        rows = []
        # df.columns levels: level 0 = field (Open/High/Low/Close/Volume), level 1 = ticker
        try:
            stacked = df.stack(level=1, future_stack=True).reset_index()
            stacked.columns = [str(c) for c in stacked.columns]
        except Exception as e:
            print(f"      stack error: {e} — skipped.")
            continue

        # Find the ticker column name and date column name
        date_col   = stacked.columns[0]
        ticker_col = stacked.columns[1]

        for _, r in stacked.iterrows():
            vt  = r[ticker_col]
            pid = vt_to_pid.get(vt)
            if not pid:
                continue
            if pd.isna(r.get("Open")) or pd.isna(r.get("Close")):
                continue
            d = r[date_col]
            if hasattr(d, "strftime"):
                d = d.strftime("%Y-%m-%d")
            rows.append({
                "pid": pid, "date": str(d),
                "o": float(r["Open"]),  "h": float(r["High"]),
                "l": float(r["Low"]),   "c": float(r["Close"]),
                "v": int(r["Volume"]) if not pd.isna(r.get("Volume")) else 0,
            })

        if args.dry_run:
            print(f"      [dry-run] would insert {len(rows):,} rows.")
        else:
            try:
                n = insert_chunk(engine, vendor_id, rows)
                print(f"      inserted {n:,} rows.")
                total_rows += n
            except SQLAlchemyError as e:
                print(f"      insert failed: {e}")
                failed_batch.extend(batch)

        time.sleep(BATCH_DELAY)

    print()
    print(f"  ── Done — total rows inserted: {total_rows:,}")
    if failed_batch:
        print(f"  ⚠ {len(failed_batch)} ticker(s) failed: {','.join(failed_batch[:20])}"
              + (" ..." if len(failed_batch) > 20 else ""))


if __name__ == "__main__":
    main()
