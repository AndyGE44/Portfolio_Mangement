# CLAUDE.md — Context for AI agents

This file orients Claude Code (or any LLM assistant) on the **My Quant App** repo. Read this first before touching code or the database. For human-facing docs, see [`README.md`](./README.md). For Python script details, see [`SCRIPTS.md`](./SCRIPTS.md).

---

## What this project is

A **personal** quantitative finance tool: one user (Andy), one Supabase project, vanilla JS frontend on GitHub Pages, Python sync scripts on GitHub Actions. Optimised for clarity over enterprise scale (33 transactions today, ~500 products tracked).

Tech stack:
- **Database:** Supabase Postgres 17 (project `jyzqnkiucgiufxfxjlqd` / region `us-east-2`)
- **Auth:** Supabase Email + Password
- **Backend:** Python 3.12 (SQLAlchemy + psycopg2 + yfinance + pandas), runs in GitHub Actions
- **Frontend:** Vanilla JavaScript + Supabase JS client, single-file HTML pages in `docs/`
- **CI:** GitHub Actions (`daily_sync.yml` cron + `recalc_portfolio.yml` workflow_dispatch)

---

## Repo map

```
.
├── 02_sync_prices/
│   ├── 02_yfbatch_sync_price.py    Daily OHLCV sync (cron)
│   ├── 02b_resync_history.py       One-off historical re-pull (auto_adjust=True)
│   └── config.py                   (duplicate of root config.py for this subdir)
├── 03_add_product.py               Interactive single-product add + optional backfill
├── 04_delete_product.py            Soft-delete (default) / hard-delete with safeguards
├── 05_add_products_batch.py        CSV-driven batch add
├── 06_add_fx_rate.py               USD↔HKD FX manager (manual / yfinance / CSV)
├── 07_rebuild_portfolio_stats.py   Canonical Python NAV/P&L rebuild (single connection,
│                                   bulk fetch, in-memory replay, batched upsert)
├── config.py                       DB_CONNECTION + setup_logging
├── requirements.txt
├── docs/
│   ├── index.html                  Trading terminal (chart + watchlist)
│   ├── portfolio.html              Holdings dashboard (toolbar, overview, holdings table)
│   └── config.js                   SUPABASE_URL + anon key (NOT in git — gitignored)
├── .github/workflows/
│   ├── daily_sync.yml              Weekday cron, 21:10 UTC
│   └── recalc_portfolio.yml        On-demand recalc (workflow_dispatch)
├── README.md                       Public overview
├── SCRIPTS.md                      Per-script reference
└── CLAUDE.md                       ← this file
```

The legacy `01_sync_products.py` is incompatible with the current schema and shouldn't be re-run.

---

## Supabase setup — what's in the database

Project ref: **`jyzqnkiucgiufxfxjlqd`** (use this as `project_id` for any MCP tool calls).

### Schema overview

```
                          ┌── currencies ──┐
                          │                │
           fx_rates ──────┤                ├── products ──┬── vendor_mappings ── vendors
                          │                │              │
                          └────────────────┘              ├── quotes
                                                          │
auth.users ── profiles ──┬── user_brokers ── broker       └── transactions
                         │                                       │
                         └── portfolios ──┬── transactions ──────┘
                                          ├── portfolio_holdings
                                          └── daily_portfolio_stats
```

### Tables (14, all in `public`)

| Table | Rows (≈) | RLS | Notes |
|---|---|---|---|
| `currencies` | 3 | public read | `USD`, `HKD`, `CNY` |
| `vendors` | 4 | n/a | `1=yfinance`, `2=fmp`, `3=yahoo_finance`, `4=manual` |
| `broker` | 9 | public read | Institution catalog |
| `products` | 509 | public read | Stocks + ETFs + cash + options. `asset_class IN ('stock','etf','cash','option')` |
| `option_details` | varies | public read | 1:1 satellite for `asset_class='option'` rows. PK is `product_id`. Holds `option_type` (call/put), `strike_price`, `expiration_date`, `underlying_product_id`, `contract_multiplier` (default 100) |
| `vendor_mappings` | 1.5k | n/a | `(product_id, vendor_id) → vendor_ticker` |
| `quotes` | 900k | public read | OHLCV, **stored split-adjusted** (`auto_adjust=True`) |
| `fx_rates` | 3.3k | public read | `(from, to, date) → rate`. PK includes date so we keep history |
| `profiles` | 1 | own row only | 1:1 with `auth.users` |
| `user_brokers` | 3 | own rows only | A user's accounts at brokers |
| `portfolios` | 2 | own rows only | `base_currency`, `start_date` |
| `transactions` | 33 | via portfolio | Source of truth — everything else is derived |
| `portfolio_holdings` | 9 | via portfolio | Trigger-maintained snapshot |
| `daily_portfolio_stats` | varies | via portfolio | Python-rebuilt or PL/pgSQL-rebuilt; `total_nav` is `GENERATED` |

### Enums

```sql
transaction_type        = buy | sell | deposit | withdrawal
                        | dividend | fee | tax | interest
                        | exchange | split
                        | exercise | assignment | expiration   -- options lifecycle
market_data_source_type = realtime | delayed | eod | manual_fix
```

### Functions (11)

| Function | Lang | Purpose |
|---|---|---|
| `txn_qty_delta(type, qty)` | SQL `IMMUTABLE` | Signed quantity per transaction type. `buy/deposit/dividend/interest/exchange/split → +qty`; `sell/withdrawal/fee/tax/exercise/assignment/expiration → −qty` (the option-life types close the option position) |
| `apply_txn_to_holding(pid, prod, broker, qty, price, type)` | PL/pgSQL | Single source of truth for per-transaction holding mutation. Weighted-avg cost on additions; deletes row at zero qty |
| `transactions_sync_holdings_fn()` | PL/pgSQL trigger | INSERT applies incrementally; UPDATE/DELETE full-rebuilds the affected portfolio |
| `rebuild_portfolio_holdings(pid)` | PL/pgSQL | Wipe + replay one portfolio's holdings |
| `rebuild_all_holdings()` | PL/pgSQL | Loop above over all portfolios |
| `rebuild_portfolio_stats(p_portfolio_id, p_from?, p_to?)` | PL/pgSQL `SECURITY DEFINER` | In-database rebuild of `daily_portfolio_stats`. Multiplier-aware (joins `option_details`). Auth-checked via `auth.uid()`. Returns the number of days rebuilt. **Called via `supabase.rpc(...)` from the Recalc button.** |
| `upsert_cash_quotes(date)` | PL/pgSQL | `open=high=low=close=1.0` for every cash product |
| `get_fx_rate(date, from, to)` | SQL `STABLE` | Latest rate ≤ `date`; returns `1.0` if `from = to` |
| `get_latest_dates()` | SQL | Internal helper used by the index page |
| `handle_new_user()` | `SECURITY DEFINER` trigger | Creates `profiles` row on signup |
| `rls_auto_enable()` | utility | Helper used during initial RLS rollout |

> Daily NAV/P&L can be rebuilt **two ways** that produce identical output: (a) `supabase.rpc('rebuild_portfolio_stats', {p_portfolio_id})` — runs in-database, sub-second, called by the Recalc button; (b) `python 07_rebuild_portfolio_stats.py --portfolio <id> --full` — Python equivalent, kept as a backup and also runnable via the `recalc_portfolio.yml` GitHub Actions workflow.

### Triggers (5)

| Trigger | Table | When | Purpose |
|---|---|---|---|
| `transactions_sync_holdings` | `public.transactions` | `AFTER INSERT/UPDATE/DELETE` | Keep `portfolio_holdings` in lock-step with `transactions` |
| `on_auth_user_created` | `auth.users` | `AFTER INSERT` | Auto-create `profiles` row |
| `handle_updated_at` | `quotes` | `BEFORE UPDATE` | Refresh `updated_at` |
| `handle_updated_at` | `portfolio_holdings` | `BEFORE UPDATE` | Refresh `updated_at` |
| `handle_updated_at` | `daily_portfolio_stats` | `BEFORE UPDATE` | Refresh `updated_at` |

### Row-Level Security (full policy table)

```
broker                  · SELECT · USING (true)                                           -- public
fx_rates                · SELECT · USING (true)                                           -- public
products                · SELECT · USING (true)                                           -- public
quotes                  · SELECT · USING (true)                                           -- public
profiles                · SELECT · USING (auth.uid() = id)
profiles                · UPDATE · USING (auth.uid() = id)
portfolios              · SELECT · USING (auth.uid() = user_id)
portfolios              · INSERT · WITH CHECK (auth.uid() = user_id)
portfolios              · UPDATE · USING (auth.uid() = user_id)
portfolios              · DELETE · USING (auth.uid() = user_id)
user_brokers            · SELECT · USING (auth.uid() = user_id)
user_brokers            · INSERT · WITH CHECK (auth.uid() = user_id)
user_brokers            · UPDATE · USING (auth.uid() = user_id)
user_brokers            · DELETE · USING (auth.uid() = user_id)
transactions            · ALL    · USING (portfolio_id IN (SELECT id FROM portfolios WHERE user_id = auth.uid()))
portfolio_holdings      · ALL    · same pattern via portfolio
daily_portfolio_stats   · ALL    · same pattern via portfolio
```

> The Python scripts use the **service-role connection string** (`DB_CONNECTION` env var / GitHub Secret) which bypasses RLS. The browser uses the **anon key** plus a logged-in JWT, which respects RLS.

### Reference data shipped with DB

| Table | Values |
|---|---|
| `currencies` | `USD`, `HKD`, `CNY` |
| `vendors` | `yfinance`(1), `fmp`(2), `yahoo_finance`(3), `manual`(4) |
| `broker` | Futu, Fidelity, Charles Schwab, Interactive Brokers, E*TRADE, Robinhood, Vanguard, Other, WeBull |
| Cash `products` | `USD`(id=3), `HKD`(id=4) — `asset_class='cash'` |

---

## Critical project conventions

### 1. Cash-as-product — and the post-2026-04 typing migration

Cash balances are tracked as positions in `asset_class='cash'` products. The `transaction_type` for the cash side of a trade is **NOT** `deposit`/`withdrawal`:

| Event | Underlying / option leg | Cash leg |
|---|---|---|
| External cash IN | — | **`deposit`** on cash product |
| External cash OUT | — | **`withdrawal`** on cash product |
| Stock buy | `buy` on stock | **`sell`** on cash product (`qty × price`) |
| Stock sell | `sell` on stock | **`buy`** on cash product (`qty × price`) |
| FX swap | — | `sell` on src ccy + `buy` on dst ccy |
| Dividend / interest | — | `dividend` / `interest` on cash product |
| Fee / tax | — | `fee` / `tax` on cash product |
| Stock split | `split` on stock (quantity = *extra* shares) | — |
| Option buy | `buy` on option (`qty=contracts`, `price=premium`) | **`sell`** on cash (`qty × price × contract_multiplier`) |
| Option sell | `sell` on option | **`buy`** on cash (`qty × price × contract_multiplier`) |
| Option exercise (long) | `exercise` on option (closes position) + stock leg at strike (`buy` for call, `sell` for put) | mirror of stock leg |
| Option assignment (short) | `assignment` on option (closes position) + stock leg at strike (`sell` for call, `buy` for put) | mirror of stock leg |
| Option expires worthless | `expiration` on option (closes position; price=0) | — |

Why: this lets `net_flows` be computed as `WHERE transaction_type IN ('deposit','withdrawal')` without double-counting internal cash movements.

The web frontend auto-creates cash legs with the correct typing. Anything that inserts transactions outside the frontend MUST follow the same convention.

### 2. Quotes are split-adjusted

`yfinance` is called with `auto_adjust=True` everywhere. Holdings are replayed as a simple `Σ txn_qty_delta(type, qty)` (post-split share count); the adjusted historical close keeps split-day NAV invariant. **Never mix raw and adjusted prices.** If you change the daily-sync script, keep `auto_adjust=True` and re-run `02b_resync_history.py` to fix history.

### 3. FX cost basis is locked at transaction date

The two calculation paths agree on this:
- **Python** (`07_rebuild_portfolio_stats.py`): looks up `fx_at(d)` for each transaction's date when computing per-day market_value, cash_balance, net_flows.
- **JS** (`docs/portfolio.html`): `buildPositions()` accumulates `costUSD` using the FX rate at each transaction's date; market value uses today's FX. The diff captures FX P&L correctly.

If you write any new code that compares cost basis to market value, **never** convert both at today's FX — that hides FX P&L.

### 4. Where the calculation lives

| Output | Where | FX |
|---|---|---|
| `daily_portfolio_stats` rows (Last Day P&L tile) | `rebuild_portfolio_stats` PL/pgSQL fn — called via `supabase.rpc(...)` from the Recalc button OR via `07_rebuild_portfolio_stats.py` (backup) | Historical |
| Live overview & holdings dashboard | `docs/portfolio.html` JS (browser) | Cost locked at txn date; MV at today |

For options, all three paths multiply by `option_details.contract_multiplier` when computing market value (`qty × close × multiplier × fx`).

### 4b. Options: market value & no-quote behavior

- An option position's market value uses the same yfinance OCC ticker (e.g. `AMD260619C00200000`) that's stored on the product. The daily sync includes `asset_class='option'` so quotes are pulled automatically.
- If no quote exists yet (newly created contract or illiquid strike), the holdings table renders `MV = —` and the live NAV simply omits the position from market value. The `daily_portfolio_stats` rebuild treats the missing close as `NULL`, so the day-end MV contribution is 0 until a quote arrives.
- Cost basis is `qty × premium × multiplier` (not `qty × premium`). The `submitTxn` cash-leg auto-fan-out applies this — anything that inserts option transactions outside the frontend MUST do the same.

### 5. The recalc button trigger path

`docs/portfolio.html` → `supabase.rpc('rebuild_portfolio_stats', { p_portfolio_id })` → PL/pgSQL function (SECURITY DEFINER, auth-checked via `auth.uid()`) → upserts `daily_portfolio_stats` → JS auto-refreshes the dashboard.

This runs in-database in well under a second; **no GitHub PAT required** anymore.

The legacy GitHub-Actions path (`recalc_portfolio.yml` → `07_rebuild_portfolio_stats.py`) is kept as a backup. To trigger it manually from the Actions tab, the workflow needs `Actions: Read and write` permission.

---

## How to do common things

### Run a script locally

```bash
# .env should contain DB_CONNECTION (Supabase pooler URL — bypasses RLS)
python 07_rebuild_portfolio_stats.py --portfolio 2 --full --dry-run
```

### Apply a DB migration via Supabase MCP

```jsonc
// project_id is always 'jyzqnkiucgiufxfxjlqd'
{
  "name": "snake_case_migration_name",
  "query": "ALTER TABLE …; CREATE FUNCTION …;"
}
```

Use `apply_migration` for DDL; `execute_sql` for one-off queries. **Never** put generated IDs in migrations.

### Inspect the DB

- `mcp__…__list_tables` (`schemas: ['public']`, `verbose: true`) — full schema dump
- `execute_sql` for ad-hoc queries
- Tables you read often: `transactions`, `daily_portfolio_stats`, `fx_rates`, `quotes`

### Add a new product

```bash
python 03_add_product.py --ticker TICKER          # interactive
# or
python 05_add_products_batch.py products.csv      # batch CSV
```

Both auto-fetch from yfinance and write `vendor_mappings` for IDs 1 and 3.

### Add an FX rate

```bash
python 06_add_fx_rate.py --fetch 2026-04-26 2026-04-26   # one day
python 06_add_fx_rate.py --date 2026-04-26 --rate 7.785  # manual
```

Both directions (USD→HKD and HKD→USD) are written together.

---

## Common pitfalls (please don't repeat these)

1. **Don't use `deposit`/`withdrawal` for cash legs of trades.** Use `buy`/`sell` on the cash product. The Python rebuild filters external flows by transaction type only.
2. **Don't compute principal at today's FX.** It hides FX P&L. Use the FX rate at each contributing transaction's date.
3. **Don't write to `daily_portfolio_stats` directly.** Run `07_rebuild_portfolio_stats.py`. The schema has a generated `total_nav` column; manual inserts will reject if you supply it.
4. **Don't mix `auto_adjust=False` and `True` in `quotes`.** Pick one (we picked True). Re-sync the affected range with `02b_resync_history.py`.
5. **Don't bypass the trigger by inserting into `portfolio_holdings` directly.** Insert into `transactions` and let the trigger handle it. If you must, follow with `SELECT rebuild_portfolio_holdings(pid)`.
6. **Don't put secrets in `docs/`.** That directory is published via GitHub Pages. The Supabase anon key is fine; the service-role key and `DB_CONNECTION` are not.
7. **Don't query `quotes` without `vendor_id = 3`.** That column is part of the PK and there can be multiple vendors per `(product, date, source_type)`.
8. **Don't push without considering `.gitignore`.** `.env`, `*.log`, `__pycache__`, `.claude` are blocked. Don't unblock them.
9. **Don't forget the contract multiplier on options.** Cost basis & cash-leg amounts are `qty × premium × multiplier` (default 100), but the option product's `quantity` is contract count (not multiplied). Same rule for market value: `qty × close × multiplier × fx`.
10. **Don't insert an option transaction without a matching `option_details` row.** The frontend's "+ Option" panel creates both atomically — if you bypass it, do `INSERT INTO products(asset_class='option')` then `INSERT INTO option_details(...)` in the same migration.

---

## When making changes

- **Schema change?** Apply a migration via Supabase MCP `apply_migration`. Update `README.md` + `CLAUDE.md` schema sections.
- **Conventions change?** Update the cash-leg/FX-leg table in **all four places**: `README.md`, `SCRIPTS.md`, `CLAUDE.md`, and the in-code comment in `07_rebuild_portfolio_stats.py`.
- **New script?** Add a section to `SCRIPTS.md` and a row to the repo map in `README.md`.
- **Frontend change that touches the calculation?** Make sure the JS path agrees with the Python path on FX handling.
- **New transaction type?** Update `txn_qty_delta` (PL/pgSQL function) AND `rebuild_portfolio_stats` PL/pgSQL CASE blocks AND `TXN_SIGN` in `07_rebuild_portfolio_stats.py` AND `TXN_SIGN` in `docs/portfolio.html`. Four places, one logical change.

---

## Useful one-liners

```bash
# Latest stats date in DB
psql -c "SELECT MAX(stat_date) FROM daily_portfolio_stats;"

# Active stock products with no quotes in last 7 days (sync gaps)
SELECT p.ticker FROM products p WHERE p.is_active AND p.asset_class='stock'
  AND NOT EXISTS (SELECT 1 FROM quotes q
    WHERE q.product_id = p.id AND q.vendor_id = 3
      AND q.trade_date >= CURRENT_DATE - 7);

# Find inconsistent holdings (trigger drift)
-- compare portfolio_holdings vs replay; rebuild if diff
SELECT rebuild_portfolio_holdings(<id>);
```

---

## User profile

Andy:
- Owner / sole user of this app.
- Holds USD + HKD cash; trades primarily US-listed equities through Futu_HK and WeBull HK.
- Wants meaningful FX P&L visibility (HKD swings against USD matter).
- Prefers Python over PL/pgSQL for non-trivial logic (testability + debuggability), even if SQL would be shorter.
- Iterates fast; expects the assistant to do the whole plan when asked, not just propose it.
