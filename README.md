# My Quant App

A personal quantitative finance platform built on Supabase (PostgreSQL). It syncs daily stock prices, tracks multi-currency portfolios across broker accounts, and serves a trading-terminal UI.

---

## Architecture

```
GitHub Actions (daily cron)
  └── 02_sync_prices/02_yfbatch_sync_price.py   ← OHLCV via yfinance
  └── upsert_cash_quotes(CURRENT_DATE)           ← cash prices = 1.0

Supabase (PostgreSQL)
  ├── Market data   : products, quotes, vendor_mappings, vendors
  ├── Reference     : currencies, broker, fx_rates
  ├── Auth & users  : auth.users (Supabase built-in), profiles
  ├── Portfolio     : user_brokers, portfolios, transactions
  └── Derived       : portfolio_holdings, daily_portfolio_stats

Frontend (docs/index.html)
  └── Vanilla JS + Supabase client + Lightweight Charts
      ← candlestick chart, watchlist, daily change %
```

---

## Database Schema

### Reference & Market Data

| Table | Description |
|---|---|
| `currencies` | ISO currency codes (USD, HKD, …) |
| `broker` | Institution catalog — Futu, Fidelity, Schwab, IBKR, etc. (shared, public read) |
| `vendors` | Price data sources: `yfinance`, `fmp`, `yahoo_finance`, `manual` |
| `products` | Tradeable assets (S&P 500 stocks + cash products like USD, HKD). `asset_class` distinguishes `'stock'` from `'cash'` |
| `vendor_mappings` | Maps a product to its vendor-specific ticker (e.g. `BRK.B` → `BRK-B` for FMP) |
| `quotes` | Daily OHLCV price data. PK: `(product_id, trade_date, source_type, vendor_id)` |
| `fx_rates` | Daily FX rates: `(from_currency, to_currency, rate_date) → rate` |

### Users & Auth

| Table | Description |
|---|---|
| `auth.users` | Managed by Supabase Auth — stores hashed passwords, JWTs, email confirmation |
| `profiles` | Public extension of `auth.users`. `id` is a 1-to-1 FK. Holds `username`, `display_name` |

A trigger (`on_auth_user_created`) auto-creates a `profiles` row whenever a new user signs up.

### Portfolio

| Table | Description |
|---|---|
| `user_brokers` | A user's brokerage accounts. Links `user_id → profiles` and `broker_id → broker` (institution). Supports multiple accounts per user |
| `portfolios` | A named portfolio belonging to a user (`user_id → profiles`). Holds a base currency |
| `transactions` | Every trade/cash event. References `portfolio_id`, `account_id` (→ `user_brokers`), and `product_id` |
| `portfolio_holdings` | **Derived snapshot** of current positions. Kept live by a trigger on `transactions`. PK: `(portfolio_id, product_id, user_broker_id)` |
| `daily_portfolio_stats` | Daily NAV, market value, cash balance, PnL, and daily return per portfolio |

#### Cash-as-Product convention
Cash balances are tracked as holdings of special `asset_class = 'cash'` products (e.g. product `USD`, `HKD`). A stock purchase inserts two transaction rows: one `buy` on the stock, one `withdrawal` on the cash product. This keeps the schema uniform — `portfolio_holdings` needs no special cash column.

#### Relationships

```
profiles ──< user_brokers >── broker (institution)
profiles ──< portfolios
portfolios ──< transactions >── user_brokers
                            └── products
portfolios ──< portfolio_holdings >── user_brokers
                                  └── products
portfolios ──< daily_portfolio_stats
```

---

## Database Functions

### Market Data

#### `upsert_cash_quotes(p_trade_date DATE) → INTEGER`
Inserts `open=high=low=close=1.0, volume=0, source_type='eod'` into `quotes` for every `asset_class='cash'` product on the given date. Uses the `manual` vendor. Returns the number of rows upserted.

Called automatically each weekday by GitHub Actions after the regular price sync.

```sql
SELECT upsert_cash_quotes(CURRENT_DATE);
SELECT upsert_cash_quotes('2026-01-15');
```

#### `get_fx_rate(p_from DATE, p_from_ccy VARCHAR, p_to_ccy VARCHAR) → NUMERIC`
Looks up the most recent FX rate on or before `p_from`. Returns `1.0` automatically when `p_from_ccy = p_to_ccy` (no self-lookup needed).

```sql
SELECT get_fx_rate(CURRENT_DATE, 'HKD', 'USD');  -- HKD per 1 USD
SELECT get_fx_rate(CURRENT_DATE, 'USD', 'USD');  -- → 1.0
```

---

### Holdings Sync

The `portfolio_holdings` table is a cached snapshot derived from `transactions`. Two complementary mechanisms keep it correct:

#### Option B (default): Trigger — incremental, live

**`transactions_sync_holdings_fn()`** fires `AFTER INSERT OR UPDATE OR DELETE` on `transactions`.

- **INSERT** → calls `apply_txn_to_holding()` incrementally (fast, O(1))
- **UPDATE / DELETE** → falls back to `rebuild_portfolio_holdings()` for that portfolio, because edits are order-sensitive and unsafe to unwind mathematically

The trigger is transparent — insert a transaction and the holding is updated automatically.

#### Option A (recovery): Full recompute

##### `rebuild_portfolio_holdings(p_portfolio_id INTEGER) → VOID`
Wipes `portfolio_holdings` for one portfolio and replays all its transactions in chronological order through `apply_txn_to_holding()`. Use this after a bulk import, a manual data fix, or any suspected drift.

```sql
SELECT rebuild_portfolio_holdings(1);  -- rebuild portfolio id=1
```

##### `rebuild_all_holdings() → INTEGER`
Calls `rebuild_portfolio_holdings()` for every portfolio in the system. Returns the count of portfolios rebuilt. Nuclear recovery option.

```sql
SELECT rebuild_all_holdings();
```

---

### Holdings Core Logic

#### `apply_txn_to_holding(p_portfolio_id, p_product_id, p_user_broker_id, p_quantity, p_price, p_fx_rate, p_txn_type) → VOID`
The single source of truth for how a transaction affects a holding row. Called by both the trigger and the rebuild function, so the math is defined exactly once.

Rules:
- **Opening / reopening a position** (`delta > 0` and current qty ≤ 0): sets `avg_cost_local` and `avg_fx_rate` to the new transaction's values (cost basis reset)
- **Adding to a position** (`delta > 0` and current qty > 0): computes a new weighted average
- **Reducing a position** (`delta < 0`): quantity decreases, cost basis unchanged
- **Position reaches zero**: the holding row is deleted

#### `txn_qty_delta(p_type transaction_type, p_quantity NUMERIC) → NUMERIC`
Returns the signed quantity effect of a transaction type. Used by `apply_txn_to_holding` and available for queries.

| Transaction type | Qty effect |
|---|---|
| `buy`, `deposit`, `dividend`, `interest` | `+quantity` |
| `sell`, `withdrawal`, `fee`, `tax` | `−quantity` |
| `exchange` | `+quantity` (caller sets sign via quantity) |

```sql
SELECT txn_qty_delta('sell', 100);   -- → -100
SELECT txn_qty_delta('deposit', 50); -- → 50
```

---

### Auth Triggers

#### `handle_new_user()` — trigger on `auth.users`
Fires `AFTER INSERT` on `auth.users`. Inserts a matching row in `public.profiles` with the same `id`. Runs as `SECURITY DEFINER` so it can write to `profiles` regardless of the signing user's permissions.

---

## Row-Level Security (RLS)

All portfolio tables are locked with RLS. The pattern:

| Table | Access rule |
|---|---|
| `profiles` | SELECT / UPDATE own row only (`auth.uid() = id`) |
| `portfolios` | All operations own rows only (`user_id = auth.uid()`) |
| `user_brokers` | All operations own rows only (`user_id = auth.uid()`) |
| `transactions` | Via portfolio ownership |
| `portfolio_holdings` | Via portfolio ownership |
| `daily_portfolio_stats` | Via portfolio ownership |
| `products`, `quotes`, `fx_rates`, `broker` | Public read, no write |

---

## Data Pipeline

### Daily price sync (`daily_sync.yml`)
Runs weekdays at **4:10 PM EST** (21:10 UTC) via GitHub Actions.

1. **`02_yfbatch_sync_price.py`** — downloads OHLCV for all S&P 500 products via `yfinance` batch download; upserts into `quotes`
2. **`upsert_cash_quotes(CURRENT_DATE)`** — inserts price=1.0 for USD and HKD cash products

Required GitHub secret: `DB_CONNECTION` (PostgreSQL connection string).

### One-off scripts

| Script | Purpose |
|---|---|
| `01_sync_products.py` | Scrape S&P 500 list from Wikipedia, populate `products` and `vendor_mappings` |

---

## Frontend

`docs/index.html` — static single-file trading terminal (GitHub Pages).

- Left panel: watchlist sorted by turnover, live search filter
- Center: interactive candlestick + volume chart (Lightweight Charts)
- Right: latest OHLCV data for selected ticker

Config in `docs/config.js`:

```js
const CONFIG = {
    SUPABASE_URL: '...',
    SUPABASE_KEY: '...',  // anon/public key
    VENDOR_ID: 3,         // yahoo_finance
    ASSET_CLASS: 'stock',
    DEFAULT_TICKER: 'NVDA'
};
```

---

## Authentication

Login uses Supabase Email+Password Auth:

```js
// Sign in
const { data, error } = await supabaseClient.auth.signInWithPassword({
  email: 'user@example.com',
  password: 'password'
});

// Sign up (auto-creates profiles row via trigger)
const { data, error } = await supabaseClient.auth.signUp({
  email: 'user@example.com',
  password: 'password'
});
```

After sign-in, all Supabase queries automatically respect RLS and are scoped to the authenticated user's data.

---

## Environment Variables

| Variable | Required | Description |
|---|---|---|
| `DB_CONNECTION` | Yes | PostgreSQL connection string (Supabase pooler URL) |
| `FMP_API_KEY` | No | Financial Modeling Prep API key (backup price source only) |
