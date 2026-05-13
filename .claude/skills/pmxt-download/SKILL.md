---
name: pmxt-download
description: Download and filter PMXT Polymarket orderbook archive data. Use when the user asks to get, download, or fetch PMXT archive data, orderbook data, or Polymarket market data for specific assets, timeframes, or date ranges.
argument-hint: <natural language request, e.g. "BTC 5min markets for the last 3 days">
---

# PMXT Data Download

You help users download filtered Polymarket orderbook data from the PMXT archive. Parse the user's natural language request into a config, run the download pipeline, and report results.

## Project location

The pmxt-data-ingestion scripts are at: `/Users/clarkpalmer/dev/pmxt-data-ingestion`

## How to use this skill

### Step 1: Parse the request

Extract from `$ARGUMENTS` (or the conversation):
- **Assets**: btc, eth, sol, xrp, doge, bnb, hype, etc.
- **Duration**: 5m, 15m, 1h, 4h (default: 5m)
- **Date range**: absolute dates or relative ("last 3 days", "yesterday", "today")
- **Other selectors**: slugs, condition IDs, event IDs, tags

For relative dates, compute from today's date:
!`date -u +%Y-%m-%d`

### Step 2: Update config.yaml

Read the current config:
!`cat /Users/clarkpalmer/dev/pmxt-data-ingestion/config.yaml`

Then edit `/Users/clarkpalmer/dev/pmxt-data-ingestion/config.yaml` with the parsed parameters. Use `YYYY-MM-DD` for full days or `YYYY-MM-DDTHH` for specific hours (UTC).

Example for "BTC and ETH 5min for the last 3 days":
```yaml
data_dir: data
download:
  connections: 4
  temp_dir: /tmp/pmxt_ingestion
start_date: "<3 days ago>"
end_date: "<yesterday>"
markets:
- asset: btc
  duration: 5m
- asset: eth
  duration: 5m
```

### Step 3: Run the pipeline

Run each step and show the user the output:

```bash
cd /Users/clarkpalmer/dev/pmxt-data-ingestion

# Discover condition IDs
python download.py --discover

# Download and filter
python download.py

# Generate report
python report.py
```

Each step can take minutes for large date ranges. Show progress as it runs.

### Step 4: Report results

After completion, summarize:
- How many condition IDs were discovered
- How many hours of data were downloaded
- Total rows and file sizes
- Show the report output

## Supported assets and durations

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
- The `data/checkpoint.json` tracks progress — safe to interrupt and resume.
- If the user's network blocks PMXT (SafeBrowse/Xfinity), they may need to set `HTTPS_PROXY=socks5h://127.0.0.1:1080` before running.
