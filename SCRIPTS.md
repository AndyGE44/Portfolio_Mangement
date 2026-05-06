# Scripts Reference

Python scripts for managing market data, products, FX, and portfolio statistics.  
All scripts share `config.py` (root) for database connectivity and run against the same Supabase Postgres database.

---

## Table of Contents

1. [Environment Setup](#environment-setup)
2. [config.py — Shared Configuration](#configpy--shared-configuration)
3. [01_sync_products.py — S&P 500 Bootstrap](#01_sync_productspy--sp-500-bootstrap)
4. [02_sync_prices/02_yfbatch_sync_price.py — Daily Price Sync](#02_sync_prices02_yfbatch_sync_pricepy--daily-price-sync)
5. [02_sync_prices/02b_resync_history.py — One-off Historical Re-sync](#02_sync_prices02b_resync_historypy--one-off-historical-re-sync)
6. [03_add_product.py — Add Single Product](#03_add_productpy--add-single-product)
7. [04_delete_product.py — Delete / Deactivate Product](#04_delete_productpy--delete--deactivate-product)
8. [05_add_products_batch.py — Batch Add from CSV](#05_add_products_batchpy--batch-add-from-csv)
9. [06_add_fx_rate.py — Add USD ↔ HKD FX Rates](#06_add_fx_ratepy--add-usd--hkd-fx-rates)
10. [07_rebuild_portfolio_stats.py — Daily NAV / P&L (Canonical)](#07_rebuild_portfolio_statspy--daily-nav--pl-canonical)
11. [Shared: Vendor IDs](#shared-vendor-ids)
12. [Shared: backfill() function](#shared-backfill-function)
13. [Shared: Cash-leg / FX-leg conventions](#shared-cash-leg--fx-leg-conventions)

---

## Environment Setup

All scripts read the database connection string from the environment.

**Local development** — create a `.env` file in the project root:

```
DB_CONNECTION=postgresql://postgres.<ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres
FMP_API_KEY=your_fmp_key   # optional, only needed for FMP price source
```

**GitHub Actions** — the connection string is injected as a repository secret (`DB_CONNECTION`).  
The `config.py` module calls `load_dotenv()` automatically, so the same code works in both environments.

**Install dependencies:**

```bash
pip install -r requirements.txt
```

---

## config.py — Shared Configuration

**Location:** `config.py` (root), `02_sync_prices/config.py` (duplicate for that subdirectory)

Loaded by every script via `import config`.

| Export | Type | Description |
|---|---|---|
| `DB_CONNECTION` | `str` | PostgreSQL connection string. Raises `ValueError` at import time if missing. |
| `FMP_API_KEY` | `str \| None` | FMP API key. `None` if not set. |
| `setup_logging(name, log_file)` | function | Returns a `logging.Logger` that writes to both a file and stdout. |

### `setup_logging(name, log_file)`

```python
logger = config.setup_logging("MyScript", "my_script.log")
logger.info("Starting...")
```

Creates a logger named `name`, writing formatted `%(asctime)s - %(levelname)s - %(message)s`
lines to `log_file` (relative to CWD) and to stdout simultaneously.

---

## 01_sync_products.py — S&P 500 Bootstrap

> **Note:** This is the original one-time bootstrap script written when the schema used
> `symbol` and `type` columns. The current schema uses `ticker` and `asset_class` instead.
> This script is **not compatible with the current schema** and should not be re-run as-is.
> It is kept for reference. Use `03_add_product.py` or `05_add_products_batch.py` for new products.

**Purpose:** Scrapes the S&P 500 constituent list from Wikipedia and populates the `products`
and `vendor_mappings` tables in bulk using the `fmp` vendor.

### How it works

1. **Fetch constituent list** — sends a browser-spoofed HTTP request to the Wikipedia
   [List of S&P 500 companies](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies)
   page and parses the first HTML table with `pandas.read_html`.
2. **Ticker normalisation** — converts dots to dashes to match FMP's ticker format
   (e.g. `BRK.B` → `BRK-B`).
3. **Upsert products** — inserts each company as a `stock` product with `base_currency='USD'`.
   On conflict, updates the name only.
4. **Upsert vendor mappings** — links each product to the `fmp` vendor with the normalised ticker.
   On conflict, does nothing (preserves any manually corrected mapping).
5. **Logging** — writes to `sync_products.log` and stdout.

### Key function

#### `get_sp500_list_from_wiki() → list[dict]`

Returns a list of dicts with keys `symbol`, `name`, `sector`. Returns `[]` on any HTTP or
parse error.

---

## 02_sync_prices/02_yfbatch_sync_price.py — Daily Price Sync

**Purpose:** Downloads the previous trading day's OHLCV data for all active stock, ETF and
**option** products (`asset_class IN ('stock','etf','option')`) and upserts it into the
`quotes` table. Runs automatically every weekday at **4:10 PM EST** via the `daily_sync.yml`
GitHub Actions workflow.

**Usage (manual trigger):**

```bash
cd 02_sync_prices
python 02_yfbatch_sync_price.py
```

### Configuration constants

| Constant | Value | Description |
|---|---|---|
| `EOD_CUTOFF_HOUR` | `16` | Hour (ET) after which a quote is considered end-of-day |
| `EOD_CUTOFF_MINUTE` | `5` | Minute component of the cutoff |
| `SOURCE_TYPE` | `'eod'` | Written into `quotes.source_type` |

### How it works

1. **Resolve vendor** — queries `vendors` for the `yahoo_finance` row and gets its `id`.
2. **Fetch active products** — joins `products` and `vendor_mappings` to get every active
   stock / ETF / option product along with its `yahoo_finance` vendor ticker. Options use
   their OCC ticker (e.g. `AMD260619C00200000`); yfinance recognises this format.
3. **Bulk download** — calls `yf.download(tickers_list, auto_adjust=True, threads=True, ...)`
   once for all tickers. `auto_adjust=True` returns split- and dividend-adjusted prices, so
   replaying transactions with the post-split share count never produces a phantom NAV
   jump on split dates. This is significantly faster than one request per ticker.
4. **Stack and filter** — unstacks the multi-index DataFrame with `.stack(level="Ticker")`,
   then skips rows where `Open` or `Close` is NaN (halted stocks) and skips today's data
   if the script runs before the EOD cutoff.
5. **Chunked upsert** — batches rows into groups of 5 000 and writes each chunk as a single
   multi-row `INSERT … ON CONFLICT DO UPDATE` statement to minimise round-trips.

### EOD cutoff logic

If the script is triggered before 4:05 PM ET, today's intraday bar is excluded — only
yesterday's completed bar is written. This prevents a partial intraday close from being
stored as a final EOD price.

### Quotes upsert conflict key

```
(product_id, trade_date, source_type, vendor_id)
```

On conflict, `open`, `high`, `low`, `close`, `volume` are all overwritten.
This allows the daily cron to self-correct if a partial bar was written earlier.

---

## 02_sync_prices/02b_resync_history.py — One-off Historical Re-sync

**Purpose:** Re-pulls quote history with `auto_adjust=True` after the codebase switched away
from raw prices. Use this once when migrating from un-adjusted prices, or any time you need
to reset a date range to clean adjusted data.

**Usage:**

```bash
# Resync ALL active stock products from 2010 onward
python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-26

# Limit to a single ticker (or a comma-separated list)
python 02_sync_prices/02b_resync_history.py --start 2020-01-01 --end 2026-04-26 \
       --tickers AAPL,MSFT,NVDA

# Limit by asset class
python 02_sync_prices/02b_resync_history.py --start 2020-01-01 --end 2026-04-26 \
       --asset-class etf

# Preview deletes + downloads without writing
python 02_sync_prices/02b_resync_history.py --start 2010-01-01 --end 2026-04-26 --dry-run
```

### Arguments

| Argument | Required | Description |
|---|---|---|
| `--start YYYY-MM-DD` | Yes | Range start (inclusive) |
| `--end YYYY-MM-DD` | Yes | Range end (inclusive) |
| `--asset-class CLASS` | No | Limit to products of this `asset_class` (e.g. `stock`, `etf`) |
| `--tickers A,B,C` | No | Comma-separated display tickers to limit to |
| `--batch-size N` | No (default 50) | Tickers per `yf.download` call |
| `--dry-run` | No | Show row counts without modifying the DB |

### How it works

1. **Resolve targets** — joins `products` + `vendor_mappings` for vendor `yahoo_finance`,
   filters by `is_active = TRUE` and excludes `asset_class = 'cash'`.
2. **Delete pass** — removes existing rows in `quotes` for those products in the date range
   under `vendor_id = 3` and `source_type = 'eod'`. Idempotent.
3. **Download + insert pass** — batches tickers (default 50 per call), calls
   `yf.download(..., auto_adjust=True)`, stacks the multi-index DataFrame, drops NaN rows,
   chunks at 5 000 rows per `INSERT … ON CONFLICT DO UPDATE` statement.
4. **Sleep `BATCH_DELAY` (0.5 s)** between batches to be polite to Yahoo's servers.

> ⚠️ This wipes the date range first. Run with `--dry-run` once if unsure of scope.

---

## 03_add_product.py — Add Single Product

**Purpose:** Interactively adds one new product to the `products` table and creates
`vendor_mappings` entries for both the `yahoo_finance` and `yfinance` vendors.
Optionally backfills historical OHLCV prices.

**Usage:**

```bash
# Fully interactive
python 03_add_product.py

# Pre-supply the ticker (remaining fields still prompted)
python 03_add_product.py --ticker KIWI.NZ

# Add and immediately backfill historical prices
python 03_add_product.py --ticker AAPL --backfill 2015-01-01
```

### Arguments

| Argument | Description |
|---|---|
| `--ticker TICKER` | Display ticker (e.g. `AAPL`, `KIWI.NZ`). Prompted if omitted. |
| `--backfill DATE` | Backfill OHLCV from this date (YYYY-MM-DD). Asked interactively if omitted. |

### Interactive flow

```
── Add New Product ──────────────────────────────────────
  Display ticker []: KIWI.NZ
  Fetching info for 'KIWI.NZ' from yfinance ...
  Found: Kiwi Property Group Limited  |  exchange=NZE  |  currency=NZD

  Vendor ticker for yfinance [...]: 
  Valid currencies in DB: HKD, USD
  Company / product name [Kiwi Property Group Limited]: 
  Exchange [NZE]: 
  Base currency [NZD]: USD
  Asset class (stock/cash/etf/…) [stock]: 

  ┌─ Summary ──────────────────────────────────────────┐
  │  Display ticker : KIWI.NZ
  │  Vendor ticker  : KIWI.NZ
  │  Name           : Kiwi Property Group Limited
  │  Exchange       : NZE
  │  Currency       : USD
  └────────────────────────────────────────────────────┘
  Proceed? [Y/n]:
```

### Step-by-step logic

1. **Display ticker** — the ticker stored in `products.ticker` and shown in the UI.
2. **yfinance auto-lookup** — fetches `longName`, `exchange`, and `currency` from
   `yf.Ticker(ticker).info`. The user can accept all defaults with Enter.
3. **Vendor ticker** — defaults to the display ticker. Override only when the yfinance
   feed uses a different symbol (e.g. `BRK.B` → `BRK-B`).
4. **Valid currencies** — queries the `currencies` table and prints valid codes so the
   FK constraint is not a surprise.
5. **Duplicate check** — if the ticker already exists in `products`, offers to update the
   vendor mappings only (no re-insert of the product row).
6. **Insert** — writes to `products`, then upserts `vendor_mappings` for vendor IDs 1
   (`yfinance`) and 3 (`yahoo_finance`). Uses `ON CONFLICT DO UPDATE` so it is safe to
   re-run.
7. **Backfill** — if requested, calls the shared `backfill()` helper (see below).

### `backfill(engine, product_id, vendor_ticker, start)`

Downloads the full OHLCV history from `start` via `yf.download` and upserts into `quotes`
under vendor_id 3 (`yahoo_finance`) using the same chunked upsert as the daily sync.
Chunk size: 2 000 rows.

---

## 04_delete_product.py — Delete / Deactivate Product

**Purpose:** Removes or deactivates a product. Two modes:

| Mode | Flag | Effect |
|---|---|---|
| **Soft delete** | *(default)* | Sets `products.is_active = false`. All history preserved. |
| **Hard delete** | `--hard` | Physically removes the product and all dependent rows. |

**Usage:**

```bash
# Soft delete (safe — just hides the product from search and future syncs)
python 04_delete_product.py
python 04_delete_product.py --ticker KIWI.NZ

# Hard delete (removes quotes + vendor mappings; blocked if transactions exist)
python 04_delete_product.py --ticker KIWI.NZ --hard

# Hard delete including portfolio transactions and holdings
python 04_delete_product.py --ticker KIWI.NZ --hard --force
```

### Arguments

| Argument | Description |
|---|---|
| `--ticker TICKER` | Ticker to delete. Prompted interactively if omitted. |
| `--hard` | Physical deletion. Refused automatically if live transactions exist. |
| `--force` | Combined with `--hard`: also deletes transactions and holdings. |

### What the script shows before acting

```
  Product found:
    id          : 42
    ticker      : KIWI.NZ
    name        : Kiwi Property Group Limited
    currency    : USD
    exchange    : NZE
    asset class : stock
    status      : ACTIVE

  Dependent rows:
    quotes             : 1,250
    vendor_mappings    : 2
    transactions       : 0
    portfolio_holdings : 0
```

### Hard delete safety gates

The hard delete requires **two** separate opt-ins to affect portfolio data:

1. `--hard` alone deletes only `quotes` and `vendor_mappings`.
2. If `transactions` or `portfolio_holdings` rows exist, the script **exits with an error**
   unless `--force` is also supplied.
3. Regardless of flags, the final prompt requires you to **type the ticker by hand**
   as confirmation — it cannot be bypassed with Enter.

### Deletion order

To satisfy FK constraints, rows are deleted in this order:

```
portfolio_holdings → transactions → quotes → vendor_mappings → products
```

### Soft delete effect on other systems

| System | Behaviour after soft delete |
|---|---|
| Daily price sync | Product excluded (sync filters by `is_active = true` via `vendor_mappings` join) |
| UI ticker search | Product hidden (`fetchTickers` filters `is_active = true`) |
| Existing transactions | Unaffected — all history preserved |
| Hard delete later | Still possible; just run with `--hard` |

---

## 05_add_products_batch.py — Batch Add from CSV

**Purpose:** Reads a CSV file and inserts multiple products in one run. Behaves like
`03_add_product.py` for each row — auto-fetching missing fields from yfinance and
optionally backfilling historical prices.

**Usage:**

```bash
# Print a ready-to-edit sample CSV
python 05_add_products_batch.py --sample > products.csv

# Dry run — shows what would be inserted without touching the DB
python 05_add_products_batch.py products.csv --dry-run

# Normal run
python 05_add_products_batch.py products.csv

# Skip yfinance lookup (use only CSV data)
python 05_add_products_batch.py products.csv --no-fetch

# Override the backfill start date for every row
python 05_add_products_batch.py products.csv --backfill 2020-01-01
```

### Arguments

| Argument | Description |
|---|---|
| `csv` | Path to the input CSV file. |
| `--dry-run` | Preview the resolved rows without writing to the DB. |
| `--no-fetch` | Skip yfinance auto-lookup. All blank fields stay blank. |
| `--backfill DATE` | Force backfill start date for every product (overrides per-row `backfill_start`). |
| `--sample` | Print a sample CSV to stdout and exit. |

### CSV format

```csv
ticker,vendor_ticker,name,exchange,currency,asset_class,backfill_start
AAPL,,,,,stock,2020-01-01
MSFT,,,,,stock,2020-01-01
BRK.B,BRK-B,Berkshire Hathaway Inc.,NMS,USD,stock,
KIWI.NZ,,,,,stock,
SPY,,,,,etf,2018-01-01
```

| Column | Required | Default | Description |
|---|---|---|---|
| `ticker` | **Yes** | — | Display ticker stored in `products.ticker`. Rows with no ticker are skipped. |
| `vendor_ticker` | No | same as `ticker` | Feed ticker for yfinance/yahoo_finance. Only needed when it differs from the display ticker. |
| `name` | No | auto-fetched | Company or product name. |
| `exchange` | No | auto-fetched | Exchange code (e.g. `NMS`, `NYQ`). |
| `currency` | No | auto-fetched, falls back to `USD` | ISO currency code. Must exist in the `currencies` table. |
| `asset_class` | No | `stock` | `stock`, `etf`, `cash`, or any value that makes sense for your schema. |
| `backfill_start` | No | no backfill | YYYY-MM-DD. If present (and `--no-fetch` is not used), downloads OHLCV history from this date. |

### How it works

1. **Read CSV** — parses with `csv.DictReader`. Rows missing a `ticker` are skipped with a
   warning.
2. **Auto-fetch** — for every row that has at least one blank field among `name`, `exchange`,
   `currency`, the script calls `yf.Ticker(vendor_ticker).info`. A **350 ms delay** is
   inserted between calls to avoid triggering yfinance rate limits.
3. **Preview table** — prints a formatted summary of all resolved rows before prompting for
   confirmation.
4. **Currency validation** — queries `currencies` and skips any row whose resolved currency
   is not in the table (rather than failing the whole batch).
5. **Upsert** — for each valid row:
   - Inserts into `products` (skips if ticker already exists, but still refreshes vendor mappings).
   - Upserts `vendor_mappings` for vendor IDs 1 and 3 via `ON CONFLICT DO UPDATE`.
6. **Per-row backfill** — if `backfill_start` is set (or `--backfill` is supplied) and
   the product was newly inserted, runs the same chunked upsert used by the daily sync.
7. **Summary** — prints inserted / skipped (existed) / error counts.

### Rate limiting note

yfinance's `.info` endpoint is undocumented and unofficial. For large CSV files (100+ tickers),
consider splitting the run or increasing `YF_DELAY_S` at the top of the script if you encounter
`429 Too Many Requests` errors.

---

---

## 06_add_fx_rate.py — Add USD ↔ HKD FX Rates

**Purpose:** Inserts USD↔HKD exchange rates into the `fx_rates` table.
Always writes **both directions** in one operation — if you supply the USD→HKD rate,
the HKD→USD inverse is computed automatically (`1 / rate`).

Three modes: manual entry, yfinance auto-fetch, and CSV import.

**Usage:**

```bash
# Interactive — prompts for date(s) and rate(s); leave date blank to finish
python 06_add_fx_rate.py

# Single date via flags (non-interactive)
python 06_add_fx_rate.py --date 2025-01-15 --rate 7.7850

# Auto-fetch daily closing rates from yfinance for a date range
python 06_add_fx_rate.py --fetch 2024-01-01 2024-12-31

# Fetch but skip dates already in the DB (append-only)
python 06_add_fx_rate.py --fetch 2020-01-01 2025-12-31 --skip-existing

# Preview what would be inserted without writing
python 06_add_fx_rate.py --fetch 2024-01-01 2024-12-31 --dry-run

# Import from CSV
python 06_add_fx_rate.py --csv fx_rates.csv

# Print a sample CSV and exit
python 06_add_fx_rate.py --sample
```

### Arguments

| Argument | Description |
|---|---|
| `--date YYYY-MM-DD` | Single date for manual mode. Requires `--rate`. |
| `--rate RATE` | USD→HKD rate for `--date`. |
| `--fetch START END` | Download daily closing rates from yfinance (`USDHKD=X`) for the given range. |
| `--csv FILE` | Import from a CSV file with columns `date`, `usd_hkd`. |
| `--dry-run` | Print what would be inserted without touching the DB. |
| `--skip-existing` | (fetch / csv modes) Skip dates that already have a row in `fx_rates`. |
| `--sample` | Print a sample CSV to stdout and exit. |

### CSV format

```csv
date,usd_hkd
2024-01-02,7.8094
2024-01-03,7.8121
2024-01-04,7.8050
```

Only `date` and `usd_hkd` are needed. The inverse HKD→USD row is always computed.

### How it works

1. **Collects pairs** `(date_str, usd_hkd_rate)` from whichever mode is active.
2. **Prints a preview table** showing USD→HKD and the computed HKD→USD before writing.
3. **Upserts both rows** per date in a single `INSERT … ON CONFLICT DO UPDATE` statement:
   - `(USD, HKD, date)` → supplied rate
   - `(HKD, USD, date)` → `round(1 / rate, 10)`
4. On conflict, overwrites `rate`, `vendor_id`, and `updated_at`.

### Vendor IDs used

| Mode | Vendor | ID |
|---|---|---|
| Manual / CSV | `manual` | 4 |
| `--fetch` | `yahoo_finance` | 3 |

### `upsert_pair(conn, date_str, usd_hkd, vendor_id)`

Core helper. Writes exactly two rows per call using a single multi-row `INSERT`.
Used by all three modes.

### `fetch_from_yfinance(start, end) → list[tuple[str, float]]`

Downloads `USDHKD=X` closing prices via `yf.download`.
Returns a list of `(date_str, close_price)` tuples with NaN rows dropped.

### `existing_dates(conn) → set[str]`

Returns the set of dates already present in `fx_rates` for the `USD→HKD` direction.
Used by `--skip-existing` to avoid redundant overwrites during incremental backfills.

### Daily sync integration

Step 6 of `daily_sync.yml` runs automatically every weekday after the price sync:

```yaml
python 06_add_fx_rate.py --fetch $(date -d "2 days ago" +%Y-%m-%d) $(date -d "tomorrow" +%Y-%m-%d) --skip-existing
```

The 3-day window (2 days ago → tomorrow) ensures the correct trading day is captured
regardless of minor timing drift. `--skip-existing` prevents overwriting any rates that
were already fetched or manually entered.

---

## 07_rebuild_portfolio_stats.py — Daily NAV / P&L (Canonical Python; backup path)

**Purpose:** Rebuilds the `daily_portfolio_stats` table from `transactions` + `quotes` +
`fx_rates`. There are now **two interchangeable implementations** of the same logic:

1. **In-database:** `rebuild_portfolio_stats(p_portfolio_id, p_from?, p_to?)` PL/pgSQL
   function (`SECURITY DEFINER`, auth-checked via `auth.uid()`). Called by the **↻ Recalc**
   button via `supabase.rpc(...)`. Sub-second on a year of data.
2. **This Python script:** equivalent logic, kept as a backup and for batch jobs.

If you change the formula in either path, change it in the other — they MUST agree.

Used by:
- The **daily cron** (step 4 of `daily_sync.yml`) — `--date today`
- The **on-demand recalc workflow** (`recalc_portfolio.yml`, kept as a backup) —
  `--portfolio <id> --full`
- Local dry-runs against the prod DB (`--dry-run`)

**Usage:**

```bash
# Single day for all portfolios (cron mode)
python 07_rebuild_portfolio_stats.py --date 2026-04-26

# All portfolios, today
python 07_rebuild_portfolio_stats.py

# Date range
python 07_rebuild_portfolio_stats.py --from 2025-10-31 --to 2026-04-26

# Full rebuild from a portfolio's start_date through today
python 07_rebuild_portfolio_stats.py --portfolio 2 --full

# Preview without writing
python 07_rebuild_portfolio_stats.py --portfolio 2 --full --dry-run
```

### Arguments

| Argument | Description |
|---|---|
| `--portfolio N` | Limit to portfolio id `N`. Default: all portfolios. |
| `--date YYYY-MM-DD` | Single date (default: today). |
| `--from YYYY-MM-DD` | Range start (inclusive). |
| `--to YYYY-MM-DD` | Range end (inclusive; default: today). |
| `--full` | Each portfolio: from its `start_date` through today. |
| `--dry-run` | Print results table; no DB writes. |

### Daily formulas (per portfolio, per day, in base ccy)

```
market_value(d) = Σ over non-cash positions of  qty × close(d) × multiplier × fx_at(d)
                  (multiplier = option_details.contract_multiplier for options, else 1)
cash_balance(d) = Σ over cash positions     of  qty × fx_at(d)
total_nav(d)    = market_value + cash_balance       (generated column)
net_flows(d)    = Σ external-flow signed qty × fx_at(d)   on day d only
                  (external = transaction_type IN ('deposit','withdrawal'))
daily_pnl(d)    = total_nav(d) − total_nav(d−1) − net_flows(d)
daily_return(d) = daily_pnl / (total_nav(d−1) + net_flows(d))
                  -- start-of-day-flow convention; 0 if denominator is 0
```

`txn_qty_delta` is inlined as a `CASE` expression in the SQL replay query — the script does
NOT depend on the Postgres function (so it survives if the function is ever renamed).

For options, `load_transactions` LEFT JOINs `option_details` to expose
`contract_multiplier` per product; the Python market-value loop multiplies by it. The
PL/pgSQL `rebuild_portfolio_stats` does the same join into a temp table.

### Performance design (single-connection / bulk-fetch)

The script opens **one** DB connection per run and pre-fetches everything:

| Round-trip | What |
|---|---|
| 1 | List of portfolios (or one row if `--portfolio` set) |
| 2 | All transactions for the portfolio (sorted) |
| 3 | All FX-rate history for relevant currencies |
| 4 | All quote history for held stock products (vendor 3) |
| 5 | Seed `prev_nav` from the last existing stat row before `from_date` |
| 6 | One multi-row `INSERT … ON CONFLICT DO UPDATE` (chunked at 1000 rows) |

The per-day loop is then pure Python with bisect lookups (`Series.at(d)`). No per-day SQL.
Reads + writes wrap in a single `engine.begin()` so the whole run is one transaction.

### Conventions assumed

- `deposit` / `withdrawal` are **external-only** (not used for cash legs of trades).
- Cash legs of stock buy/sell and FX legs are stored as `buy` / `sell` on the cash product.
- Quotes use `auto_adjust=True` (split-adjusted) so split-day NAV is invariant.

If your DB does not yet follow these conventions, run the migration in
`backfill_cash_leg_typing` first (see git history) and re-pull quotes via
`02b_resync_history.py`.

### Triggering from the web

`docs/portfolio.html` calls Supabase directly — no GitHub PAT, no workflow dispatch:

```js
const { data, error } = await sb.rpc('rebuild_portfolio_stats', {
    p_portfolio_id: currentPortfolioId,
});
// data === number of days rebuilt
```

The RPC is allowed by RLS because `rebuild_portfolio_stats` is `SECURITY DEFINER` and
gates ownership via `auth.uid()`. Runs in well under a second on a year of data.

The legacy `recalc_portfolio.yml` GitHub Actions path still works (run from the Actions
tab) and is kept as a backup. It needs the `DB_CONNECTION` repo secret.

---

## Shared: Vendor IDs

| ID | Name | Used by |
|---|---|---|
| 1 | `yfinance` | Library-level identifier |
| 2 | `fmp` | Financial Modeling Prep (legacy bootstrap only) |
| 3 | `yahoo_finance` | **Daily price sync** — this is the critical mapping |
| 4 | `manual` | Cash quote upserts (`upsert_cash_quotes`) |

When adding a product, vendor mappings are always written for IDs **1 and 3**.
The daily sync (`02_yfbatch_sync_price.py`) only reads from vendor ID **3** (`yahoo_finance`),
so a product without a `yahoo_finance` mapping will never get prices synced.

---

## Shared: `backfill()` function

Implemented identically in `03_add_product.py` and `05_add_products_batch.py`.

```
backfill(engine, product_id, vendor_ticker, start)
```

| Parameter | Description |
|---|---|
| `engine` | SQLAlchemy engine |
| `product_id` | `products.id` of the target product |
| `vendor_ticker` | Ticker string passed to `yf.download` |
| `start` | Start date string, e.g. `"2020-01-01"` |

Rows with `NaN` open or close are silently dropped.  
Inserts using `source_type='eod'` and `vendor_id=3` (`yahoo_finance`), matching the daily sync.  
Chunk size is 2 000 rows per `INSERT` statement.  
Calls `yf.download(..., auto_adjust=True)` so the backfilled history is split-adjusted.

---

## Shared: Cash-leg / FX-leg conventions

Important convention shared across `06_add_fx_rate.py`, the web frontend, and
`07_rebuild_portfolio_stats.py`:

| Event | Stored as |
|---|---|
| External cash deposit / withdrawal | `deposit` / `withdrawal` on cash product |
| Cash leg of a stock **buy** | `sell` on cash product (cash decreased, amount = `qty × price`) |
| Cash leg of a stock **sell** | `buy` on cash product (cash increased, amount = `qty × price`) |
| FX swap (sell ccy A, buy ccy B) | `sell` on cash product A + `buy` on cash product B |
| Dividend / interest received | `dividend` / `interest` on cash product |
| Fee / tax paid | `fee` / `tax` on cash product |
| Stock split | `split` on the stock (quantity = *extra* shares only) |
| Option **buy** | `buy` on option (`qty=contracts`, `price=premium`) + cash leg `sell` on cash, amount = `qty × price × contract_multiplier` |
| Option **sell** | `sell` on option + cash leg `buy` on cash with the same multiplier-aware amount |
| Option **exercise** (long) | `exercise` on option (closes position; `price=0`) + stock leg at strike (`buy` on call, `sell` on put) + matching cash leg |
| Option **assignment** (short) | `assignment` on option (closes position; `price=0`) + stock leg at strike (`sell` on call, `buy` on put) + matching cash leg |
| Option **expiration** (worthless) | `expiration` on option (closes position; `price=0`) — no cash leg |

Why: this lets the rebuild paths compute external `net_flows` simply with
`WHERE transaction_type IN ('deposit','withdrawal')` — internal cash movements (including
option premium / settlement) never get double-counted as external flows. **Option cash
legs MUST be multiplied by `contract_multiplier`** — the frontend does this automatically
in `submitTxn`; any other writer must do the same.

The web frontend (`docs/portfolio.html`) auto-creates the cash leg with the correct type
when the user submits a buy/sell/FX through the form. If you insert raw rows via SQL or
another script, follow the same convention.
