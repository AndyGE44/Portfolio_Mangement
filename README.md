# My Quant App

A personal quantitative finance platform built on **Supabase (PostgreSQL)** + a static **GitHub Pages** frontend. It syncs daily stock prices, tracks multi-currency portfolios across multiple broker accounts, and computes daily NAV / P&L.

---

## Architecture

```
┌─────────────────────────── GitHub Actions (cron + dispatch) ───────────────────────────┐
│  daily_sync.yml      (weekdays 21:10 UTC)                                              │
│    1. 02_yfbatch_sync_price.py       — OHLCV via yfinance (auto_adjust=True)           │
│       includes asset_class IN ('stock','etf','option') — option quotes synced too      │
│    2. upsert_cash_quotes(today)      — cash products priced at 1.0                     │
│    3. 06_add_fx_rate.py --fetch …    — daily USD↔HKD rate                              │
│    4. 07_rebuild_portfolio_stats.py  — refresh today's daily_portfolio_stats           │
│                                                                                        │
│  recalc_portfolio.yml (workflow_dispatch — backup, kept for batch jobs)                │
│    └─ 07_rebuild_portfolio_stats.py --portfolio <id> --full                            │
│                                                                                        │
│  keepalive.yml       (Sun + Wed 08:17 UTC)                                             │
│    └─ pings Supabase + keeps the repo/cron from being auto-disabled                    │
│                                                                                        │
│  backfill.yml        (workflow_dispatch — replays a date range after an outage)        │
│    └─ 02b_resync_history.py → cash quotes → FX → 07_rebuild_portfolio_stats.py         │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────── Supabase (Postgres + Auth + RLS + REST) ────────────────────────┐
│  Reference   currencies · vendors · broker · fx_rates                                  │
│  Market data products · option_details · vendor_mappings · quotes                      │
│  Auth/Users  auth.users (built-in) · profiles                                          │
│  Portfolio   user_brokers · portfolios · transactions                                  │
│  Derived     portfolio_holdings  · daily_portfolio_stats                               │
│                                                                                        │
│  Functions   txn_qty_delta · apply_txn_to_holding · transactions_sync_holdings_fn      │
│              rebuild_portfolio_holdings · rebuild_all_holdings                         │
│              rebuild_portfolio_stats  ← in-DB rebuild, called by Recalc button         │
│              upsert_cash_quotes · get_fx_rate · handle_new_user                        │
│  Triggers    transactions → portfolio_holdings (auto-sync)                             │
│              auth.users → profiles (auto-create)                                       │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │  (REST + Supabase JS client, RLS-scoped)
                                         ▼
┌──────────────────────────────── Frontend (docs/) ──────────────────────────────────────┐
│  index.html       — trading terminal (watchlist + candlestick chart)                   │
│  portfolio.html   — portfolio dashboard (light theme):                                 │
│                       • toolbar with portfolio switcher                                │
│                       • overview metrics (NAV, P&L, principal, cash, broker NAV)       │
│                       • holdings table with expandable per-broker breakdown            │
│                       • collapsible forms for + Portfolio / + Broker / + Txn /         │
│                         + Option (create OCC contract) / Cash / FX                     │
│                       • + Txn supports buy/sell/dividend/split + exercise/             │
│                         assignment/expiration with auto multi-leg fan-out              │
│                       • ↻ Recalc → supabase.rpc('rebuild_portfolio_stats')             │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Quick start

1. **Create a Supabase project**, paste the connection string into `.env`:
   ```bash
   echo "DB_CONNECTION=postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres" > .env
   ```
2. **Apply schema + RLS + functions** from this repo's migrations (Supabase MCP or `supabase db push`).
3. **Bootstrap reference data** — currencies (`USD`, `HKD`, `CNY`), vendors (`yahoo_finance`, `manual`, …), brokers (Futu, IBKR, …) and cash products (`USD`, `HKD`).
4. **Add products + backfill quotes**:
   ```bash
   pip install -r requirements.txt
   python 05_add_products_batch.py products.csv --backfill 2020-01-01
   ```
5. **Add FX history** for USD↔HKD:
   ```bash
   python 06_add_fx_rate.py --fetch 2020-01-01 2026-04-26
   ```
6. **Configure the frontend** — copy `docs/config.example.js` → `docs/config.js`:
   ```js
   const CONFIG = {
       SUPABASE_URL: 'https://<ref>.supabase.co',
       SUPABASE_KEY: '<anon-public-key>',
       VENDOR_ID:    3,           // yahoo_finance
       ASSET_CLASS:  'stock',
       DEFAULT_TICKER: 'NVDA',
   };
   ```
7. **Sign up via the web UI** (Supabase Auth Email+Password), create a portfolio, add transactions, click ↻ Recalc. Recalc now runs in-database via `supabase.rpc('rebuild_portfolio_stats')` — no PAT needed.
8. **Wire up GitHub Actions** — add the `DB_CONNECTION` repo secret. The cron will run weekdays at 21:10 UTC. The legacy `recalc_portfolio.yml` workflow is kept as a backup and only needs `Actions: Read and write` if you want to invoke it from the GitHub UI.

---

## Supabase database setup

### 1. Tables

| Table | Purpose | RLS |
|---|---|---|
| `currencies` | ISO codes (USD, HKD, CNY) — small, public | public read |
| `vendors` | Price data sources (`yfinance`, `fmp`, `yahoo_finance`, `manual`) | n/a |
| `broker` | Broker institutions catalog (Futu, IBKR, …) — shared | public read |
| `products` | Tradeable assets — stocks, ETFs, cash products, **and equity options**. `asset_class IN ('stock','etf','cash','option')` | public read |
| `option_details` | 1:1 satellite for `asset_class='option'` rows. PK is `product_id`. Stores `option_type` (call/put), `strike_price`, `expiration_date`, `underlying_product_id`, `contract_multiplier` (default 100). | public read |
| `vendor_mappings` | `(product_id, vendor_id)` → vendor-specific ticker | n/a |
| `quotes` | Daily OHLCV. PK `(product_id, trade_date, source_type, vendor_id)` | public read |
| `fx_rates` | Daily FX. PK `(from_currency, to_currency, rate_date)` | public read |
| `profiles` | 1:1 with `auth.users` (display name etc.) | own row only |
| `user_brokers` | A user's brokerage accounts | own rows only |
| `portfolios` | A named portfolio with `base_currency` | own rows only |
| `transactions` | Every trade / cash event | scoped via portfolio ownership |
| `portfolio_holdings` | **Trigger-maintained** snapshot of current positions per `(portfolio, product, broker)` | scoped via portfolio ownership |
| `daily_portfolio_stats` | Daily NAV / P&L per portfolio. `total_nav` is `GENERATED ALWAYS AS (market_value + cash_balance)` | scoped via portfolio ownership |

### 2. Enums

```sql
transaction_type     = buy | sell | deposit | withdrawal
                     | dividend | fee | tax | interest
                     | exchange | split
                     | exercise | assignment | expiration   -- options lifecycle
market_data_source_type = realtime | delayed | eod | manual_fix
```

### 3. Cash-as-product convention

Cash balances are tracked as positions in `asset_class='cash'` products (one per currency: `USD`, `HKD`, …). This keeps the schema uniform — `portfolio_holdings` needs no special cash columns.

**Transaction-type rules (post-2026-04 migration):**

| Event | Underlying / option leg | Cash leg |
|---|---|---|
| External cash IN | — | `deposit` on cash product |
| External cash OUT | — | `withdrawal` on cash product |
| Stock buy | `buy` on stock | **`sell`** on cash product (NOT `withdrawal`) |
| Stock sell | `sell` on stock | **`buy`** on cash product (NOT `deposit`) |
| FX swap | — | `sell` on src ccy + `buy` on dst ccy |
| Dividend / interest | — | `dividend` / `interest` on cash product |
| Fee / tax | — | `fee` / `tax` on cash product |
| Stock split | `split` on stock (stores the *extra* shares) | — |
| Option buy | `buy` on option (`qty=contracts`, `price=premium`) | **`sell`** on cash, amount = `qty × price × contract_multiplier` |
| Option sell | `sell` on option | **`buy`** on cash, same multiplier-aware amount |
| Option exercise (long) | `exercise` on option (closes position) **+** stock leg at strike (`buy` on call, `sell` on put) | mirror of stock leg (cash flow at `shares × strike`) |
| Option assignment (short) | `assignment` on option (closes position) **+** stock leg at strike (`sell` on call, `buy` on put) | mirror of stock leg |
| Option expires worthless | `expiration` on option (closes position; `price=0`, no cash flow) | — |

Why: `deposit` / `withdrawal` are **reserved for genuinely external flows**. The rebuild scripts compute `net_flows` strictly from those two types, so internal cash movements never inflate or deflate the daily-flow figure. The frontend's "+ Txn" auto-creates the cash leg with the correct typing **and** applies the contract multiplier for options — anything that inserts transactions outside the frontend MUST do the same.

### 4. Functions

| Function | Purpose |
|---|---|
| `txn_qty_delta(type, qty)` | Signed quantity effect of a transaction type. `buy/deposit/dividend/interest/exchange/split → +qty`; `sell/withdrawal/fee/tax/exercise/assignment/expiration → −qty` |
| `apply_txn_to_holding(...)` | Single source of truth for how one transaction mutates a `portfolio_holdings` row. Implements weighted-avg cost on additions, deletes the row at zero qty. |
| `transactions_sync_holdings_fn()` | Trigger function fired `AFTER INSERT/UPDATE/DELETE` on `transactions`. INSERTs apply incrementally; UPDATEs/DELETEs full-rebuild that portfolio. |
| `rebuild_portfolio_holdings(pid)` | Replay all transactions for one portfolio in chronological order. Use after bulk imports or suspected drift. |
| `rebuild_all_holdings()` | Loop the above over every portfolio. |
| `rebuild_portfolio_stats(p_portfolio_id, p_from?, p_to?)` | `SECURITY DEFINER` PL/pgSQL function. In-database rebuild of `daily_portfolio_stats`. Multiplier-aware (joins `option_details`). Auth-checked via `auth.uid()`. Returns the number of days rebuilt. **Called via `supabase.rpc(...)` from the Recalc button** — sub-second on a year of data. |
| `upsert_cash_quotes(date)` | Insert `open=high=low=close=1.0` rows into `quotes` for every cash product on a given date. Run by the daily cron. |
| `get_fx_rate(date, from_ccy, to_ccy)` | Latest rate ≤ `date`. Returns `1.0` if `from_ccy = to_ccy`. |
| `handle_new_user()` | `SECURITY DEFINER` trigger on `auth.users` — auto-creates a matching `profiles` row on signup. |

> **Recalc has two paths that produce identical output:** the Recalc button calls `supabase.rpc('rebuild_portfolio_stats')` (in-database, fast); the GitHub Actions workflow `recalc_portfolio.yml` runs `07_rebuild_portfolio_stats.py` (kept as a backup). The Python path is also useful for local dry-runs against the prod DB.

### 5. Triggers

| Trigger | On | Effect |
|---|---|---|
| `transactions_sync_holdings` | `AFTER INSERT/UPDATE/DELETE ON transactions` | Keeps `portfolio_holdings` in lock-step with `transactions` |
| `on_auth_user_created` | `AFTER INSERT ON auth.users` | Creates the `profiles` row |
| `handle_updated_at` | `BEFORE UPDATE ON quotes / portfolio_holdings / daily_portfolio_stats` | Sets `updated_at = NOW()` |

### 6. Row-Level Security (RLS) — concrete policies

| Table | Cmd | Policy `USING` (and/or `WITH CHECK`) |
|---|---|---|
| `profiles` | SELECT, UPDATE | `auth.uid() = id` |
| `portfolios` | ALL (4 separate policies) | `auth.uid() = user_id` |
| `user_brokers` | ALL (4 separate policies) | `auth.uid() = user_id` |
| `transactions` | ALL | `portfolio_id IN (SELECT id FROM portfolios WHERE user_id = auth.uid())` |
| `portfolio_holdings` | ALL | same pattern via portfolio |
| `daily_portfolio_stats` | ALL | same pattern via portfolio |
| `products`, `quotes`, `fx_rates`, `broker` | SELECT only | `true` (public read) |

> The Python sync scripts use the **service-role connection string** (the one stored in `DB_CONNECTION` / GitHub Secret), which bypasses RLS. The browser client uses the **anon key** plus a logged-in JWT, which respects RLS.

### 7. Reference data ships with the DB

```
vendors:  1=yfinance · 2=fmp · 3=yahoo_finance · 4=manual
currencies: USD · HKD · CNY
brokers:  Futu · Fidelity · Charles Schwab · Interactive Brokers ·
          E*TRADE · Robinhood · Vanguard · Other · WeBull
```

The daily price sync reads only from **vendor 3 (`yahoo_finance`)**. Cash quotes are written under **vendor 4 (`manual`)**.

---

## Data pipeline

### Daily sync (`.github/workflows/daily_sync.yml`)
Weekdays at **21:10 UTC** (16:10 ET / 17:10 EDT):

1. `02_sync_prices/02_yfbatch_sync_price.py` — bulk download OHLCV (`auto_adjust=True`, split-adjusted)
2. `SELECT upsert_cash_quotes(CURRENT_DATE)` — cash products at 1.0
3. `06_add_fx_rate.py --fetch ... --skip-existing` — pulls USD↔HKD daily close
4. `07_rebuild_portfolio_stats.py --date today` — recompute today's `daily_portfolio_stats` for all portfolios

### On-demand recalc

Two interchangeable paths produce identical output:

- **Recalc button (default)** — `docs/portfolio.html` calls `supabase.rpc('rebuild_portfolio_stats', { p_portfolio_id })`. Runs in Postgres, sub-second on a year of data, multiplier-aware for options, auth-checked via `auth.uid()`.
- **`recalc_portfolio.yml` (backup)** — `workflow_dispatch` job runs `07_rebuild_portfolio_stats.py --portfolio <id> --full`. Useful for batch jobs or local dry-runs against the prod DB.

### Keepalive (`.github/workflows/keepalive.yml`)

Two independent inactivity timers can silently kill this project, and this workflow resets both. It runs **Sundays and Wednesdays at 08:17 UTC** (and can be triggered manually):

| Timer | Who kills it | Fix in the workflow |
| --- | --- | --- |
| **7 days** with no database activity | Supabase pauses free-tier projects | `SELECT now()` against `DB_CONNECTION` — a ping every 3–4 days |
| **60 days** with no repository activity | GitHub disables all `schedule` triggers | Pushes an empty commit whenever the last commit is 30+ days old |

It also calls the Actions REST API to re-enable `daily_sync.yml` / `keepalive.yml` if it finds them in a `disabled_inactivity` state, so the pipeline can recover on its own.

> If a paused Supabase project has *already* been restored by hand, nothing else is needed — the keepalive keeps it awake from then on. If GitHub has *already* disabled the cron, the first re-enable has to be done in the **Actions** tab (a disabled workflow does not run, so it cannot re-enable itself).

### Backfill (`.github/workflows/backfill.yml`)

`02_yfbatch_sync_price.py` only ever downloads a yesterday→tomorrow window, so a manual re-run of `daily_sync.yml` cannot fill a gap left by a paused cron. Dispatch **Backfill Missing Days** with a `start` / `end` date instead — it replays the same four steps over the whole range.

> Heads-up: step 1 calls `02b_resync_history.py`, which **deletes** the existing `yahoo_finance` / `eod` quotes inside the range before re-downloading them. That is what makes the replay idempotent, but keep the range tight — pass only the days that are actually missing.

---

## Frontend

`docs/` is published via **GitHub Pages**.

- `docs/index.html` — single-file trading terminal: watchlist sorted by turnover, candlestick + volume chart (Lightweight Charts), latest OHLCV table.
- `docs/portfolio.html` — portfolio management dashboard. Top toolbar selects the active portfolio; overview tile grid + holdings table render below. The "Cash Balance" tile expands into a per-(currency × broker) breakdown that shows FX P&L. Multi-broker positions in the holdings table expand into per-account sub-rows.
- `docs/config.js` — non-secret public config (Supabase URL + anon key + display defaults). Not checked in for hygiene; copy from `config.example.js`.

### How calculations are split (important!)

| What | Lives in | FX strategy |
|---|---|---|
| `daily_portfolio_stats` rows (Last Day P&L, etc.) | `rebuild_portfolio_stats` PL/pgSQL fn (Recalc button) **or** `07_rebuild_portfolio_stats.py` (backup) | **Historical** FX at each date |
| Live overview & holdings table | `portfolio.html` JavaScript (browser) | Cost basis **locked** at each transaction's FX date; market value uses today's FX |

Both treat FX consistently, so the gap between mvUSD and the locked principal correctly captures stock P&L + FX P&L. For options, all three paths multiply by `option_details.contract_multiplier` when computing market value (`qty × close × multiplier × fx`).

---

## Authentication

Supabase Email + Password Auth. Sign-in via the web UI:

```js
await supabase.auth.signInWithPassword({ email, password });
```

The `handle_new_user` trigger auto-creates a `profiles` row on signup, so the first action after signing up can already write to `portfolios` / `user_brokers`.

---

## Environment variables

| Variable | Where | Required |
|---|---|---|
| `DB_CONNECTION` | `.env` (local) + GitHub Secret | Yes — direct Postgres connection used by all Python scripts (bypasses RLS) |
| `FMP_API_KEY` | `.env` | Optional — only needed if you ever resurrect the FMP price source |

The web frontend reads its public Supabase URL + anon key from `docs/config.js`; nothing secret on the client.

---

## Documentation

| File | Audience |
|---|---|
| `README.md` | This file — high-level overview |
| `SCRIPTS.md` | Reference for every Python script (args, behavior, file-by-file) |
| `CLAUDE.md` | Project context for AI agents (Claude Code etc.) — conventions, gotchas, where things live |
