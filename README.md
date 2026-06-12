# pmxt-data-ingestion

Download, filter, and enrich Polymarket crypto prediction market data from the [PMXT archive](https://archive.pmxt.dev).

Downloads hourly orderbook snapshots from the PMXT v2 archive, filters to only the crypto markets you care about, and optionally enriches with trade data, resolutions, and strike prices from the Polymarket APIs. Supports both v1 and v2 archive formats.

## Supported Markets

| Asset | Durations | Slug Pattern |
|-------|-----------|-------------|
| BTC | 5m, 15m | `btc-updown-{duration}-{timestamp}` |
| ETH | 5m | `eth-updown-5m-{timestamp}` |
| SOL | 5m | `sol-updown-5m-{timestamp}` |
| XRP | 5m | `xrp-updown-5m-{timestamp}` |

More assets/durations may exist — just add them to config.yaml and the script will try to discover them.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Optional: install aria2 for faster downloads (otherwise uses requests)
# macOS: brew install aria2
# Ubuntu: apt install aria2

# Edit config.yaml to set your date range and markets
# Then:

# 1. Download orderbook data (with auto-resume)
python download.py

# 2. Optionally merge daily files into one
python merge.py

# 3. Fetch trades, resolutions, and strike prices
python enrich.py

# 4. See what you've got
python report.py
```

## Configuration

Edit `config.yaml`. You can use any combination of market selectors — they all resolve to condition IDs used for filtering.

```yaml
# Archive version: "v2" (default, recommended) or "v1" (legacy)
archive_version: v2

start_date: "2026-03-21"
end_date: "2026-03-23"
data_dir: data
download:
  connections: 4
  temp_dir: /tmp/pmxt_ingestion

# Updown crypto markets (asset + duration shorthand)
markets:
  - asset: btc
    duration: 5m
  - asset: eth
    duration: 15m

# Arbitrary market slugs
slugs:
  - "will-bitcoin-reach-122k-september-15-21"
  - "nba-bos-cle-2026-03-08-spread-home-4pt5"

# Direct condition IDs (no API lookup)
condition_ids:
  - "0x94b3610eec4e5ef8..."

# Event IDs (all markets under an event)
event_ids:
  - "196660"

# Tags (search Gamma API)
tags:
  - "crypto"
```

### Market Selectors

| Selector | Description | API calls |
|----------|-------------|-----------|
| `markets` | Updown crypto shorthand (`{asset}-updown-{duration}-{ts}`) | 1 per market per time window |
| `slugs` | Any Polymarket slug | 1 per slug |
| `condition_ids` | Hex condition IDs — direct, no lookup | None |
| `event_ids` | All markets under a Polymarket event | 1 per event |
| `tags` | Search by Polymarket tag | Paginated search |

All selectors can be combined. At least one must be present.

### Archive Versions

| Version | Archive URL | Download URL | Schema |
|---------|------------|-------------|--------|
| **v2** (default) | `archive.pmxt.dev/Polymarket/v2` | `r2v2.pmxt.dev` | Flattened columns (`market`, `event_type`, `bids`, `asks`, etc.) |
| **v1** (legacy) | `archive.pmxt.dev/Polymarket` | `r2.pmxt.dev` | JSON blob (`market_id`, `update_type`, `data`) |

Both versions are supported. The download and report scripts auto-detect the parquet schema, so you can switch versions without changing anything else. Set `archive_version: v1` if you need the legacy format.

## Scripts

### `download.py` — Download & Filter Orderbook Data

Downloads hourly parquet files from the PMXT archive, filters them to only rows matching your configured markets, and saves daily parquet files. The raw archive files are ~500MB-1.5GB each; after filtering you'll have much smaller files containing only your markets.

```bash
python download.py              # Download and filter
python download.py --status     # Show download progress
python download.py --discover   # Only discover condition IDs (no download)
```

**How it works:**
1. Discovers condition IDs for your markets via the Gamma API
2. Downloads each hourly archive file
3. Filters to only rows matching your condition IDs (using DuckDB)
4. Deletes the raw file, saves the filtered chunk
5. Merges hourly chunks into daily files
6. Checkpoints after every file — safe to interrupt and resume

### `merge.py` — Merge Files

Merges all daily orderbook files into a single parquet file.

```bash
python merge.py                          # Merge all into orderbook_all.parquet
python merge.py --output my_data.parquet # Custom output name
```

### `enrich.py` — Trades, Resolutions, Strike Prices

Fetches additional data for each market from the Polymarket APIs:
- **Trades**: Every trade execution (price, size, side, timestamp)
- **Resolutions**: Whether the market resolved Up or Down
- **Strike prices**: The opening price the market was measured against

```bash
python enrich.py                # Fetch everything
python enrich.py --trades       # Only trades
python enrich.py --resolutions  # Only resolutions
python enrich.py --status       # Show progress
```

### `report.py` — Data Report

Shows what data you have: row counts, date coverage, market counts, trade volume, resolution stats, and cross-references which markets have orderbook + trades + resolutions. Saves a timestamped markdown report to `reports/`.

```bash
python report.py              # Full report (also saves to reports/)
python report.py --summary    # Short summary (orderbook + coverage only)
```

## Output Structure

```
data/
├── orderbook/
│   ├── orderbook_2026-03-21.parquet
│   ├── orderbook_2026-03-22.parquet
│   └── orderbook_2026-03-23.parquet
├── trades/
│   └── trades.parquet
├── resolutions.parquet
├── condition_ids.json          # slug → condition ID mapping
└── checkpoint.json             # download/enrichment progress
reports/
└── report_20260327_120000.md   # timestamped markdown reports
```

## Orderbook Parquet Schema

The PMXT archive has two schema versions. The download and report scripts handle both automatically.

### V2 (current)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_received` | int64 | When the update was received |
| `timestamp` | int64 | When the update was created |
| `market` | binary (UTF-8) | Condition ID (hex) identifying the market |
| `event_type` | string | `book`, `price_change`, `last_trade_price`, `tick_size_change` |
| `asset_id` | binary (UTF-8) | Token ID |
| `bids` | binary (UTF-8) | JSON bid levels |
| `asks` | binary (UTF-8) | JSON ask levels |
| `price` | int32 | Price (scaled) |
| `size` | int64 | Size |
| `side` | string | Side |
| `best_bid` | int32 | Best bid price |
| `best_ask` | int32 | Best ask price |
| `fee_rate_bps` | int32 | Fee rate in basis points |
| `transaction_hash` | binary (UTF-8) | Transaction hash |

### V1 (legacy)

| Column | Type | Description |
|--------|------|-------------|
| `timestamp_received` | timestamp | When the update was received |
| `timestamp_created_at` | timestamp | When the update was created |
| `market_id` | string | Condition ID (hex) identifying the market |
| `update_type` | string | `book_snapshot` or `price_change` |
| `data` | string | JSON payload with orderbook/price data |

**`book` / `book_snapshot`** — Full orderbook state (all levels, both sides). Fires after trades.

**`price_change`** — Order placement or cancellation (NOT a trade). Contains token_id, side, best_bid, best_ask, change details.

## Trade Parquet Schema

| Column | Type | Description |
|--------|------|-------------|
| `condition_id` | string | Market condition ID |
| `slug` | string | Market slug |
| `side` | string | BUY (taker lifted ask) or SELL (taker hit bid) |
| `price` | float | Trade price |
| `size` | float | Trade size in shares |
| `timestamp` | int | Unix timestamp |
| `outcome` | string | Which outcome token was traded |

## Known Limitations: Orderbook Coverage

The PMXT archive captures orderbook events for **all** Polymarket markets (~13,000+ per hourly file). For short-duration crypto markets (5m, 15m), orderbook activity is extremely bursty — most of it happens in a narrow window around market creation and resolution. In practice, this means:

- **The first and last hourly files of the day (T00, T23) contain the vast majority of crypto market orderbook data** — this is when market makers create and close positions for the full day's markets.
- **Mid-day hourly files have very few rows** for your crypto markets (often single digits or zero), even though the raw archive files are 200-300MB each (full of data for other Polymarket markets).
- **Typical coverage: ~30-40% of configured markets will have orderbook data.** The exact number depends on the date and market type. Hour 00 often has 90%+ coverage; mid-day hours may have 15-30%.

**Trade data and resolutions are not affected** — those come from separate Polymarket APIs and typically have 95%+ coverage.

The `report.py` script gives a detailed breakdown of what has coverage and what's missing, so you can assess data completeness before using it.

## Notes

- The PMXT archive publishes hourly. Files typically appear within a few minutes of the hour.
- Raw archive files are large (200MB-1.5GB). The script downloads one at a time, filters immediately, and deletes the raw file. Peak temp disk usage is ~1.5GB.
- Condition ID discovery via the Gamma API takes ~1 request per market at 5 req/s. For a full day of BTC 5m markets (288), this takes about a minute.
- The Polymarket Data API has an offset cap of 3000 per market. Very high-volume markets may have truncated early-window trade data.
- aria2c is optional but recommended — it uses multiple connections for ~2-4x faster downloads.
- No API keys required — all data sources (PMXT archive, Gamma API, Polymarket Data API) are public.
