#!/usr/bin/env python3
"""
Delete or deactivate a product from the database.

Two modes:
  Soft delete (default) — sets is_active=False.  Safe at any time.
                          The product stays in history; it just disappears from
                          ticker search and future price syncs.
  Hard delete  (--hard) — physically removes the product and ALL dependent rows
                          (quotes, vendor_mappings, and optionally transactions /
                          portfolio_holdings if you confirm the extra warning).
                          Irreversible. Refused automatically if live transactions
                          exist unless you also pass --force.

Usage:
    python 04_delete_product.py
    python 04_delete_product.py --ticker KIWI.NZ
    python 04_delete_product.py --ticker KIWI.NZ --hard
    python 04_delete_product.py --ticker KIWI.NZ --hard --force
"""
import sys
import argparse
import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def find_product(conn, ticker):
    return conn.execute(
        text("SELECT id, name, base_currency, exchange, asset_class, is_active FROM products WHERE ticker = :t"),
        {"t": ticker},
    ).fetchone()


def counts(conn, product_id):
    q  = conn.execute(text("SELECT COUNT(*) FROM quotes             WHERE product_id = :p"), {"p": product_id}).scalar()
    vm = conn.execute(text("SELECT COUNT(*) FROM vendor_mappings    WHERE product_id = :p"), {"p": product_id}).scalar()
    tx = conn.execute(text("SELECT COUNT(*) FROM transactions       WHERE product_id = :p"), {"p": product_id}).scalar()
    ph = conn.execute(text("SELECT COUNT(*) FROM portfolio_holdings WHERE product_id = :p"), {"p": product_id}).scalar()
    return {"quotes": q, "vendor_mappings": vm, "transactions": tx, "holdings": ph}


def main():
    parser = argparse.ArgumentParser(description="Delete or deactivate a product.")
    parser.add_argument("--ticker", help="Ticker symbol to delete, e.g. KIWI.NZ")
    parser.add_argument("--hard",   action="store_true",
                        help="Physically remove all rows (default: soft-delete only)")
    parser.add_argument("--force",  action="store_true",
                        help="Allow hard delete even when live transactions exist")
    args = parser.parse_args()

    print("\n── Delete Product ───────────────────────────────────────────────────")

    ticker = (args.ticker or "").upper() or input("  Ticker to delete: ").strip().upper()
    if not ticker:
        print("  No ticker provided. Aborted.")
        sys.exit(1)

    engine = create_engine(config.DB_CONNECTION)

    with engine.connect() as conn:
        product = find_product(conn, ticker)

    if not product:
        print(f"  '{ticker}' not found in products table.")
        sys.exit(1)

    pid, name, currency, exchange, asset_class, is_active = product
    c = {}
    with engine.connect() as conn:
        c = counts(conn, pid)

    status = "ACTIVE" if is_active else "already inactive"
    print(f"""
  Product found:
    id          : {pid}
    ticker      : {ticker}
    name        : {name}
    currency    : {currency}
    exchange    : {exchange or '—'}
    asset class : {asset_class}
    status      : {status}

  Dependent rows:
    quotes           : {c['quotes']:,}
    vendor_mappings  : {c['vendor_mappings']}
    transactions     : {c['transactions']}
    portfolio_holdings : {c['holdings']}
""")

    # ── Soft delete ───────────────────────────────────────────────────────────
    if not args.hard:
        if not is_active:
            print("  Product is already inactive. Nothing to do.")
            sys.exit(0)
        confirm = input("  Soft-delete (set is_active=False)? [Y/n]: ").strip().lower()
        if confirm not in ("", "y", "yes"):
            print("  Aborted.")
            return
        try:
            with engine.begin() as conn:
                conn.execute(
                    text("UPDATE products SET is_active = false WHERE id = :p"),
                    {"p": pid},
                )
            print(f"  Done — '{ticker}' is now inactive. All historical data preserved.")
        except SQLAlchemyError as e:
            print(f"  DB error: {e}")
        return

    # ── Hard delete ───────────────────────────────────────────────────────────
    print("  WARNING: Hard delete will permanently remove:")
    print(f"    • {c['quotes']:,} quote rows")
    print(f"    • {c['vendor_mappings']} vendor mapping(s)")
    if c["transactions"] or c["holdings"]:
        print(f"    • {c['transactions']} transaction(s)")
        print(f"    • {c['holdings']} holding row(s)")
        print()
        print("  This product has live portfolio data.")
        if not args.force:
            print("  Re-run with --force to confirm deletion of portfolio data too.")
            sys.exit(1)
        print("  --force supplied: portfolio data will also be deleted.")

    print()
    confirm = input(f"  Type the ticker '{ticker}' to confirm hard delete: ").strip().upper()
    if confirm != ticker:
        print("  Confirmation did not match. Aborted.")
        return

    try:
        with engine.begin() as conn:
            if c["holdings"]:
                conn.execute(text("DELETE FROM portfolio_holdings WHERE product_id = :p"), {"p": pid})
            if c["transactions"]:
                conn.execute(text("DELETE FROM transactions       WHERE product_id = :p"), {"p": pid})
            conn.execute(text("DELETE FROM quotes             WHERE product_id = :p"), {"p": pid})
            conn.execute(text("DELETE FROM vendor_mappings    WHERE product_id = :p"), {"p": pid})
            conn.execute(text("DELETE FROM products           WHERE id = :p"),         {"p": pid})

        print(f"  Done — '{ticker}' and all dependent rows permanently deleted.")

    except SQLAlchemyError as e:
        print(f"  DB error: {e}")


if __name__ == "__main__":
    main()
