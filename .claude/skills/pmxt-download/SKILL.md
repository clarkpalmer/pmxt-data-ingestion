---
name: pmxt-download
description: Download and filter PMXT Polymarket orderbook archive data. Use when the user asks to get, download, or fetch PMXT archive data, orderbook data, or Polymarket market data for specific assets, timeframes, or date ranges.
argument-hint: <natural language request, e.g. "BTC 5min markets for the last 3 days">
---

# PMXT Data Download

You help users download filtered Polymarket orderbook data from the PMXT archive. Parse the user's natural language request into a config, run the download pipeline, and report results.

## Project location

This skill ships inside the pmxt-data-ingestion repo — the project root is the directory containing this `.claude/` folder (it has `config.yaml`, `download.py`, `report.py`). All commands below run from that repo root.

## How to use this skill

### Step 1: Parse the request

Extract from `$ARGUMENTS` (or the conversation):
- **Assets**: btc, eth, sol, xrp, doge, bnb, hype, etc.
- **Duration**: 5m, 15m, 4h (default: 5m — see the table below for which assets have which)
- **Date range**: absolute dates or relative ("last 3 days", "yesterday", "today")
- **Other selectors**: slugs, condition IDs, event IDs, tags

For relative dates, compute from today's date:
!`date -u +%Y-%m-%d`

### Step 2: Update config.yaml

Read the repo's current `config.yaml` first, then edit ONLY the dates and market selectors with the parsed parameters — preserve all other keys (`archive_version`, `data_dir`, `download`, `markets_snapshot`, …). Use `YYYY-MM-DD` for full days or `YYYY-MM-DDTHH` for specific hours (UTC).

Example selectors for "BTC and ETH 5min for the last 3 days":
```yaml
start_date: "<3 days ago>"
end_date: "<yesterday>"
markets:
- asset: btc
  duration: 5m
- asset: eth
  duration: 5m
```

### Step 3: Make sure the markets snapshot is available (recommended)

```bash
python snapshot.py --status
```

If no snapshot exists, offer to download it (`python snapshot.py --yes`, ~600 MB, free, no API key). With a snapshot, condition-ID discovery is a ~1s local lookup instead of one rate-limited Gamma API call per market (~1 min per asset-day); markets newer than the snapshot still resolve via Gamma automatically.

### Step 4: Run the pipeline

Run each step from the repo root and show the user the output:

```bash
# Discover condition IDs
python download.py --discover

# Download and filter
python download.py

# Generate report
python report.py
```

Each step can take minutes for large date ranges. Show progress as it runs.

### Step 5: Report results

After completion, summarize:
- How many condition IDs were discovered
- How many hours of data were downloaded
- Total rows and file sizes
- Show the report output

## Supported assets and durations

(Same table as the README — verified against live Gamma discovery, June 2026. More may exist; discovery simply finds nothing for unsupported combos.)

| Asset | Durations available |
|-------|-------------------|
| BTC | 5m, 15m, 4h |
| ETH | 5m, 15m, 4h |
| SOL | 5m, 15m, 4h |
| XRP | 5m, 15m, 4h |
| DOGE | 5m |
| BNB | 5m |
| HYPE | 5m |

## Important notes

- The PMXT archive publishes hourly. Files appear within minutes of the hour.
- Raw archive files are 200MB-1.5GB each. Downloads take 1-5 minutes per file.
- Always use UTC dates/times.
- The `data/checkpoint.json` tracks progress — safe to interrupt and resume; incremental re-runs merge new hours into the existing daily files.
- If the user's network blocks `archive.pmxt.dev` (some content filters do), they can route the download through a proxy they trust, e.g. `HTTPS_PROXY=socks5h://...`.
