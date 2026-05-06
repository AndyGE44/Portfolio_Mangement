#!/usr/bin/env python3
"""
Rebuild `daily_portfolio_stats` from transactions + quotes + fx_rates.

Canonical implementation. Used by:
  - the daily cron (.github/workflows/daily_sync.yml) — `--date today`
  - the on-demand recalc (.github/workflows/recalc_portfolio.yml,
    triggered from the web by workflow_dispatch with a portfolio_id)

Conventions (post 2026-04 migration):
  • `deposit` / `withdrawal` are EXTERNAL flows only.
  • Cash legs of stock buy/sell and FX legs are stored as `buy` / `sell`
    on the cash product.
  • `split` rows store the *extra* shares (delta), positive quantity.
  • Quotes are ADJUSTED prices (auto_adjust=True), so split-day NAV is
    invariant and we can replay holdings as a simple sum.

Daily formulas (per portfolio, per day d, in base ccy):
    market_value(d) = Σ over non-cash positions  qty × close(d) × fx
    cash_balance(d) = Σ over cash positions      qty × fx
    total_nav(d)    = market_value + cash_balance
    net_flows(d)    = Σ external-flow signed qty × fx       on day d
    daily_pnl(d)    = total_nav(d) − total_nav(d-1) − net_flows(d)
    daily_return(d) = daily_pnl / (total_nav(d-1) + net_flows(d))
                       (start-of-day flow convention; 0 if denominator is 0)

Performance:
    All reads/writes happen over ONE connection. Transactions, FX history,
    quote history and seed NAVs are bulk-fetched up-front; the per-day loop
    is pure Python (bisect lookups). Final upsert is a single multi-row SQL.

Usage:
    python 07_rebuild_portfolio_stats.py                   # all portfolios, today
    python 07_rebuild_portfolio_stats.py --date 2026-04-25
    python 07_rebuild_portfolio_stats.py --from 2025-10-31 --to 2026-04-25
    python 07_rebuild_portfolio_stats.py --portfolio 2 --full
    python 07_rebuild_portfolio_stats.py --portfolio 2 --full --dry-run
"""
import argparse
import sys
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
import config
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

VENDOR_YAHOO = 3   # vendors.id where name = 'yahoo_finance'
UPSERT_CHUNK = 1000

# Sign convention for `quantity` per transaction_type
TXN_SIGN = {
    "buy":         +1,
    "sell":        -1,
    "deposit":     +1,
    "withdrawal":  -1,
    "dividend":    +1,
    "interest":    +1,
    "fee":         -1,
    "tax":         -1,
    "exchange":    +1,   # legacy; new FX uses buy/sell on cash
    "split":       +1,   # split row stores the extra shares (delta)
    "exercise":    -1,   # option lifecycle: all reduce position
    "assignment":  -1,
    "expiration":  -1,
}
EXTERNAL_TYPES = {"deposit", "withdrawal"}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def parse_date(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


class Series:
    """Date→value series with O(log N) latest-on-or-before lookup."""
    __slots__ = ("dates", "values")

    def __init__(self, pairs: list[tuple[date, Decimal]]):
        # pairs must already be sorted ascending by date
        self.dates  = [p[0] for p in pairs]
        self.values = [p[1] for p in pairs]

    def at(self, d: date) -> Decimal | None:
        i = bisect_right(self.dates, d)
        return self.values[i - 1] if i > 0 else None


# ─────────────────────────────────────────────────────────────────────────────
# Calculator
# ─────────────────────────────────────────────────────────────────────────────

class PortfolioCalculator:
    def __init__(self, engine):
        self.engine = engine

    # ── Reference data (one-shot at process start) ────────────────────────────

    def list_portfolios(self, conn, only_id: int | None) -> list[dict]:
        if only_id is not None:
            rows = conn.execute(text("""
                SELECT id, base_currency,
                       COALESCE(start_date, created_at::date) AS start_date
                FROM   portfolios WHERE id = :pid
            """), {"pid": only_id}).mappings().all()
        else:
            rows = conn.execute(text("""
                SELECT id, base_currency,
                       COALESCE(start_date, created_at::date) AS start_date
                FROM   portfolios ORDER BY id
            """)).mappings().all()
        return [dict(r) for r in rows]

    # ── Bulk data loaders (per portfolio) ─────────────────────────────────────

    def load_transactions(self, conn, portfolio_id: int) -> list[dict]:
        rows = conn.execute(text("""
            SELECT t.executed_at::date AS d,
                   t.transaction_type  AS type,
                   t.product_id        AS pid,
                   t.quantity          AS qty,
                   p.asset_class       AS asset_class,
                   p.base_currency     AS ccy,
                   COALESCE(od.contract_multiplier, 1) AS multiplier
            FROM   transactions t
            JOIN   products p ON p.id = t.product_id
            LEFT JOIN option_details od ON od.product_id = t.product_id
            WHERE  t.portfolio_id = :pid
            ORDER  BY t.executed_at, t.id
        """), {"pid": portfolio_id}).mappings().all()
        return [dict(r) for r in rows]

    def load_fx_series(self, conn, currencies: set[str], base_ccy: str,
                       upto: date) -> dict[tuple[str, str], Series]:
        """For each non-base ccy, fetch full history of (ccy → base_ccy) rates."""
        out: dict[tuple[str, str], Series] = {}
        non_base = [c for c in currencies if c and c != base_ccy]
        if not non_base:
            return out
        rows = conn.execute(text("""
            SELECT from_currency AS f, to_currency AS t, rate_date AS d, rate AS r
            FROM   fx_rates
            WHERE  to_currency = :base
              AND  from_currency = ANY(:ccys)
              AND  rate_date <= :upto
            ORDER  BY from_currency, rate_date
        """), {"base": base_ccy, "ccys": non_base, "upto": upto}).mappings().all()

        buckets: dict[tuple[str, str], list[tuple[date, Decimal]]] = defaultdict(list)
        for r in rows:
            buckets[(r["f"], r["t"])].append((r["d"], Decimal(r["r"])))
        for k, pairs in buckets.items():
            out[k] = Series(pairs)
        return out

    def load_quote_series(self, conn, product_ids: set[int],
                          upto: date) -> dict[int, Series]:
        if not product_ids:
            return {}
        rows = conn.execute(text("""
            SELECT product_id AS pid, trade_date AS d, close AS c
            FROM   quotes
            WHERE  vendor_id = :vid
              AND  product_id = ANY(:pids)
              AND  trade_date <= :upto
            ORDER  BY product_id, trade_date
        """), {"vid": VENDOR_YAHOO, "pids": list(product_ids),
               "upto": upto}).mappings().all()

        buckets: dict[int, list[tuple[date, Decimal]]] = defaultdict(list)
        for r in rows:
            buckets[r["pid"]].append((r["d"], Decimal(r["c"])))
        return {pid: Series(pairs) for pid, pairs in buckets.items()}

    def seed_prev_nav(self, conn, portfolio_id: int, from_date: date) -> Decimal | None:
        v = conn.execute(text("""
            SELECT total_nav FROM daily_portfolio_stats
            WHERE  portfolio_id = :pid AND stat_date < :d
            ORDER  BY stat_date DESC LIMIT 1
        """), {"pid": portfolio_id, "d": from_date}).scalar()
        return Decimal(v) if v is not None else None

    # ── In-memory replay ──────────────────────────────────────────────────────

    @staticmethod
    def fx_at(fx_series, base_ccy, ccy, d) -> Decimal:
        if ccy == base_ccy:
            return Decimal(1)
        s = fx_series.get((ccy, base_ccy))
        if s is None:
            raise RuntimeError(f"No FX history for {ccy}->{base_ccy} (need on/before {d}).")
        v = s.at(d)
        if v is None:
            raise RuntimeError(f"No FX rate for {ccy}->{base_ccy} on or before {d}.")
        return v

    def compute_series(self, portfolio_id: int, base_ccy: str,
                       from_date: date, to_date: date,
                       txns: list[dict],
                       fx_series, quote_series,
                       prev_nav: Decimal | None) -> list[dict]:
        # Track holdings across days. Pointer marches through the sorted txns.
        holdings: dict[int, Decimal] = defaultdict(Decimal)
        product_meta: dict[int, tuple[str, str, int]] = {}  # pid -> (asset_class, ccy, multiplier)
        for t in txns:
            product_meta[t["pid"]] = (t["asset_class"], t["ccy"], t.get("multiplier", 1))

        # Pre-bucket transactions by date for fast same-day external-flow scan
        txns_by_date: dict[date, list[dict]] = defaultdict(list)
        for t in txns:
            txns_by_date[t["d"]].append(t)
        sorted_txn_dates = sorted(txns_by_date.keys())

        # Apply all transactions strictly before `from_date` to seed holdings
        cursor = 0
        while cursor < len(sorted_txn_dates) and sorted_txn_dates[cursor] < from_date:
            for t in txns_by_date[sorted_txn_dates[cursor]]:
                holdings[t["pid"]] += Decimal(TXN_SIGN[t["type"]]) * Decimal(t["qty"])
            cursor += 1

        results: list[dict] = []
        for d in daterange(from_date, to_date):
            # Apply transactions on day d before computing EOD NAV
            while cursor < len(sorted_txn_dates) and sorted_txn_dates[cursor] == d:
                for t in txns_by_date[sorted_txn_dates[cursor]]:
                    holdings[t["pid"]] += Decimal(TXN_SIGN[t["type"]]) * Decimal(t["qty"])
                cursor += 1

            # Mark to market
            market_value = Decimal(0)
            cash_balance = Decimal(0)
            for pid, qty in holdings.items():
                if qty == 0:
                    continue
                asset_class, ccy, multiplier = product_meta[pid]
                fx = self.fx_at(fx_series, base_ccy, ccy, d)
                if asset_class == "cash":
                    cash_balance += qty * fx
                else:
                    s = quote_series.get(pid)
                    close = s.at(d) if s else None
                    if close is None:
                        # Skip silently; first day of a brand-new ticker may lack quote
                        continue
                    # multiplier is 1 for stocks/ETFs, 100 for options.
                    # Negative qty = short position → contributes negative MV.
                    market_value += qty * close * Decimal(multiplier) * fx

            # External flows on d (only deposit/withdrawal on cash products)
            net_flows = Decimal(0)
            for t in txns_by_date.get(d, []):
                if t["type"] not in EXTERNAL_TYPES or t["asset_class"] != "cash":
                    continue
                fx = self.fx_at(fx_series, base_ccy, t["ccy"], d)
                net_flows += Decimal(TXN_SIGN[t["type"]]) * Decimal(t["qty"]) * fx

            total_nav = market_value + cash_balance
            if prev_nav is not None:
                daily_pnl = total_nav - prev_nav - net_flows
                denom     = prev_nav + net_flows
                daily_ret = (daily_pnl / denom) if denom != 0 else Decimal(0)
            else:
                daily_pnl = Decimal(0)
                daily_ret = Decimal(0)

            results.append({
                "stat_date":    d,
                "market_value": market_value,
                "cash_balance": cash_balance,
                "net_flows":    net_flows,
                "daily_pnl":    daily_pnl,
                "daily_return": daily_ret,
            })
            prev_nav = total_nav

        return results

    # ── Bulk upsert ───────────────────────────────────────────────────────────

    @staticmethod
    def upsert_chunk(conn, portfolio_id: int, rows: list[dict]):
        if not rows:
            return
        ph, params = [], {}
        for j, r in enumerate(rows):
            ph.append(f"(:pid_{j},:d_{j},:mv_{j},:cash_{j},:flow_{j},:pnl_{j},:ret_{j}, NOW())")
            params.update({
                f"pid_{j}":  portfolio_id,
                f"d_{j}":    r["stat_date"],
                f"mv_{j}":   r["market_value"],
                f"cash_{j}": r["cash_balance"],
                f"flow_{j}": r["net_flows"],
                f"pnl_{j}":  r["daily_pnl"],
                f"ret_{j}":  r["daily_return"],
            })
        stmt = text(f"""
            INSERT INTO daily_portfolio_stats
                (portfolio_id, stat_date, market_value, cash_balance,
                 net_flows, daily_pnl, daily_return, updated_at)
            VALUES {','.join(ph)}
            ON CONFLICT (portfolio_id, stat_date) DO UPDATE SET
                market_value = EXCLUDED.market_value,
                cash_balance = EXCLUDED.cash_balance,
                net_flows    = EXCLUDED.net_flows,
                daily_pnl    = EXCLUDED.daily_pnl,
                daily_return = EXCLUDED.daily_return,
                updated_at   = NOW()
        """)
        conn.execute(stmt, params)

    # ── Top-level driver: do everything for one portfolio in one connection ──

    def rebuild(self, conn, port: dict, from_date: date, to_date: date,
                dry_run: bool = False) -> int:
        portfolio_id = port["id"]
        base_ccy     = port["base_currency"]

        txns = self.load_transactions(conn, portfolio_id)

        currencies   = {t["ccy"] for t in txns} | {base_ccy}
        stock_pids   = {t["pid"] for t in txns if t["asset_class"] != "cash"}

        fx_series    = self.load_fx_series(conn, currencies, base_ccy, to_date)
        quote_series = self.load_quote_series(conn, stock_pids, to_date)
        prev_nav     = self.seed_prev_nav(conn, portfolio_id, from_date)

        results = self.compute_series(
            portfolio_id, base_ccy, from_date, to_date,
            txns, fx_series, quote_series, prev_nav,
        )

        if dry_run:
            self._print_preview(portfolio_id, base_ccy, results)
            return len(results)

        for i in range(0, len(results), UPSERT_CHUNK):
            self.upsert_chunk(conn, portfolio_id, results[i:i + UPSERT_CHUNK])
        return len(results)

    @staticmethod
    def _print_preview(portfolio_id: int, base_ccy: str, rows: list[dict]):
        print(f"\n  Portfolio {portfolio_id}  ({base_ccy})")
        print(f"  {'DATE':<12} {'MKT_VAL':>14} {'CASH':>14} {'TOTAL_NAV':>14} "
              f"{'FLOWS':>12} {'PNL':>12} {'RET':>9}")
        print("  " + "─" * 92)
        for r in rows:
            tot = r["market_value"] + r["cash_balance"]
            print(f"  {str(r['stat_date']):<12} "
                  f"{float(r['market_value']):>14,.2f} "
                  f"{float(r['cash_balance']):>14,.2f} "
                  f"{float(tot):>14,.2f} "
                  f"{float(r['net_flows']):>12,.2f} "
                  f"{float(r['daily_pnl']):>12,.2f} "
                  f"{float(r['daily_return'])*100:>8.4f}%")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def resolve_range(args, p: dict, today: date) -> tuple[date, date] | None:
    if args.full:
        f, t = p["start_date"], today
    elif args.from_date or args.to_date:
        f = parse_date(args.from_date) if args.from_date else p["start_date"]
        t = parse_date(args.to_date)   if args.to_date   else today
    elif args.date:
        f = t = parse_date(args.date)
    else:
        f = t = today

    if f < p["start_date"]:
        f = p["start_date"]
    if t < f:
        return None
    return f, t


def main():
    ap = argparse.ArgumentParser(
        description="Rebuild daily_portfolio_stats from raw transactions + quotes + fx.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--portfolio", type=int, default=None,
                    help="Portfolio id. Default: all portfolios.")
    ap.add_argument("--date",      help="Single date (YYYY-MM-DD). Default: today.")
    ap.add_argument("--from",      dest="from_date", help="Range start (inclusive).")
    ap.add_argument("--to",        dest="to_date",   help="Range end (inclusive).")
    ap.add_argument("--full",      action="store_true",
                    help="Recalculate from each portfolio's start_date through today.")
    ap.add_argument("--dry-run",   action="store_true", help="Print only, don't write.")
    args = ap.parse_args()

    today  = date.today()
    engine = create_engine(config.DB_CONNECTION)
    calc   = PortfolioCalculator(engine)

    # ONE connection / ONE transaction for the whole run.
    # All reads + the final upserts happen here.
    try:
        conn_ctx = engine.connect() if args.dry_run else engine.begin()
        with conn_ctx as conn:
            portfolios = calc.list_portfolios(conn, args.portfolio)
            if not portfolios:
                print("  No portfolios found.")
                sys.exit(1)

            print(f"  Rebuild target: {len(portfolios)} portfolio(s)  "
                  f"(dry_run={args.dry_run})")
            total_days = 0
            for p in portfolios:
                rng = resolve_range(args, p, today)
                if rng is None:
                    print(f"  [{p['id']}] skip — empty range")
                    continue
                f, t = rng
                print(f"  [{p['id']}]  {f} → {t}  ({p['base_currency']})", flush=True)
                try:
                    n = calc.rebuild(conn, p, f, t, dry_run=args.dry_run)
                    print(f"        {n} day(s) {'previewed' if args.dry_run else 'upserted'}")
                    total_days += n
                except RuntimeError as e:
                    print(f"        ERROR: {e}")

            print(f"\n  Total: {total_days} day-row(s) processed.")
    except SQLAlchemyError as e:
        print(f"  DB error: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
