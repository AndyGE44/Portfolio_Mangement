# My Quant App

A personal quantitative finance platform built on **Supabase (PostgreSQL)** + a static **GitHub Pages** frontend. It syncs daily stock prices, tracks multi-currency portfolios across multiple broker accounts, and computes daily NAV / P&L.

---

## Architecture

```
┌─────────────────────────── GitHub Actions (cron + dispatch) ───────────────────────────┐
│  daily_sync.yml      (weekdays 21:10 UTC)                                              │
│    1. 02_yfbatch_sync_price.py       — OHLCV via yfinance (auto_adjust=True)           │
│    2. upsert_cash_quotes(today)      — cash products priced at 1.0                     │
│    3. 06_add_fx_rate.py --fetch …    — daily USD↔HKD rate                              │
│    4. 07_rebuild_portfolio_stats.py  — refresh today's daily_portfolio_stats           │
│                                                                                        │
│  recalc_portfolio.yml (workflow_dispatch — triggered from the web "↻ Recalc" button)   │
│    └─ 07_rebuild_portfolio_stats.py --portfolio <id> --full                            │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │
                                         ▼
┌─────────────────────── Supabase (Postgres + Auth + RLS + REST) ────────────────────────┐
│  Reference   currencies · vendors · broker · fx_rates                                  │
│  Market data products · vendor_mappings · quotes                                       │
│  Auth/Users  auth.users (built-in) · profiles                                          │
│  Portfolio   user_brokers · portfolios · transactions                                  │
│  Derived     portfolio_holdings  · daily_portfolio_stats                               │
│                                                                                        │
│  Functions   txn_qty_delta · apply_txn_to_holding · transactions_sync_holdings_fn      │
│              rebuild_portfolio_holdings · rebuild_all_holdings                         │
│              upsert_cash_quotes · get_fx_rate · handle_new_user                        │
│  Triggers    transactions → portfolio_holdings (auto-sync)                             │
│              auth.users → profiles (auto-create)                                       │
└────────────────────────────────────────┬───────────────────────────────────────────────┘
                                         │  (REST + Supabase JS client, RLS-scoped)
                                         ▼
┌──────────────────────────────── Frontend (docs/) ──────────────────────────────────────┐
│  index.html       — trading terminal (watchlist + candlestick chart)                   │
│  portfolio.html   — portfolio dashboard:                                               │
│                       • toolbar with portfolio switcher                                │
│                       • overview metrics (NAV, P&L, principal, cash, broker NAV)       │
│                       • holdings table with expandable per-broker breakdown            │
│                       • collapsible forms for add-portfolio/broker/txn/cash/FX         │
│                       • ↻ Recalc button → triggers GitHub Actions via PAT              │
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
7. **Sign up via the web UI** (Supabase Auth Email+Password), create a portfolio, add transactions, click ↻ Recalc.
8. **Wire up GitHub Actions** — add the `DB_CONNECTION` repo secret. The cron will run weekdays at 21:10 UTC. The "↻ Recalc" button needs a fine-grained PAT with `Actions: Read and write` scope on this repo (stored only in your browser's `localStorage`).

---

## Supabase database setup

### 1. Tables

| Table | Purpose | RLS |
|---|---|---|
| `currencies` | ISO codes (USD, HKD, CNY) — small, public | public read |
| `vendors` | Price data sources (`yfinance`, `fmp`, `yahoo_finance`, `manual`) | n/a |
| `broker` | Broker institutions catalog (Futu, IBKR, …) — shared | public read |
| `products` | Tradeable assets — stocks, ETFs, **and cash products** (`asset_class='cash'`) | public read |
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
market_data_source_type = realtime | delayed | eod | manual_fix
```

### 3. Cash-as-product convention

Cash balances are tracked as positions in `asset_class='cash'` products (one per currency: `USD`, `HKD`, …). This keeps the schema uniform — `portfolio_holdings` needs no special cash columns.

**Transaction-type rules (post-2026-04 migration):**

| Event | Stock leg | Cash leg |
|---|---|---|
| External cash IN | — | `deposit` on cash product |
| External cash OUT | — | `withdrawal` on cash product |
| Stock buy | `buy` on stock | **`sell`** on cash product (NOT `withdrawal`) |
| Stock sell | `sell` on stock | **`buy`** on cash product (NOT `deposit`) |
| FX swap | — | `sell` on src ccy + `buy` on dst ccy |
| Dividend / interest | — | `dividend` / `interest` on cash product |
| Fee / tax | — | `fee` / `tax` on cash product |
| Stock split | `split` (stores the *extra* shares) | — |

Why: `deposit` / `withdrawal` are **reserved for genuinely external flows**. The Python rebuild script computes `net_flows` strictly from those two types, so internal cash movements never inflate or deflate the daily-flow figure.

### 4. Functions

| Function | Purpose |
|---|---|
| `txn_qty_delta(type, qty)` | Signed quantity effect of a transaction type. `buy/deposit/dividend/interest/exchange/split → +qty`; `sell/withdrawal/fee/tax → −qty` |
| `apply_txn_to_holding(...)` | Single source of truth for how one transaction mutates a `portfolio_holdings` row. Implements weighted-avg cost on additions, deletes the row at zero qty. |
| `transactions_sync_holdings_fn()` | Trigger function fired `AFTER INSERT/UPDATE/DELETE` on `transactions`. INSERTs apply incrementally; UPDATEs/DELETEs full-rebuild that portfolio. |
| `rebuild_portfolio_holdings(pid)` | Replay all transactions for one portfolio in chronological order. Use after bulk imports or suspected drift. |
| `rebuild_all_holdings()` | Loop the above over every portfolio. |
| `upsert_cash_quotes(date)` | Insert `open=high=low=close=1.0` rows into `quotes` for every cash product on a given date. Run by the daily cron. |
| `get_fx_rate(date, from_ccy, to_ccy)` | Latest rate ≤ `date`. Returns `1.0` if `from_ccy = to_ccy`. |
| `handle_new_user()` | `SECURITY DEFINER` trigger on `auth.users` — auto-creates a matching `profiles` row on signup. |

> **Note:** `daily_portfolio_stats` is no longer maintained by a Postgres function. Daily P&L is computed by `07_rebuild_portfolio_stats.py` (Python) and upserted in bulk. The web "↻ Recalc" button triggers this script via GitHub Actions `workflow_dispatch`.

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

### On-demand recalc (`.github/workflows/recalc_portfolio.yml`)
Triggered from the web UI by clicking **↻ Recalc** on the portfolio dashboard. Runs `07_rebuild_portfolio_stats.py --portfolio <id> --full`, replaying everything from the portfolio's `start_date` through today.

---

## Frontend

`docs/` is published via **GitHub Pages**.

- `docs/index.html` — single-file trading terminal: watchlist sorted by turnover, candlestick + volume chart (Lightweight Charts), latest OHLCV table.
- `docs/portfolio.html` — portfolio management dashboard. Top toolbar selects the active portfolio; overview tile grid + holdings table render below. The "Cash Balance" tile expands into a per-(currency × broker) breakdown that shows FX P&L. Multi-broker positions in the holdings table expand into per-account sub-rows.
- `docs/config.js` — non-secret public config (Supabase URL + anon key + display defaults). Not checked in for hygiene; copy from `config.example.js`.

### How calculations are split (important!)

| What | Lives in | FX strategy |
|---|---|---|
| `daily_portfolio_stats` rows (Last Day P&L, etc.) | `07_rebuild_portfolio_stats.py` (Python, GitHub Actions) | **Historical** FX at each date |
| Live overview & holdings table | `portfolio.html` JavaScript (browser) | Cost basis **locked** at each transaction's FX date; market value uses today's FX |

Both treat FX consistently, so the gap between mvUSD and the locked principal correctly captures stock P&L + FX P&L.

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
