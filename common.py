"""Shared utilities for pmxt-data-ingestion scripts."""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

CONFIG_FILE = Path(__file__).parent / "config.yaml"
GAMMA_API = "https://gamma-api.polymarket.com/events"
DATA_API = "https://data-api.polymarket.com/trades"
PMXT_ARCHIVE = "https://archive.pmxt.dev/Polymarket"
PMXT_DOWNLOAD = "https://r2.pmxt.dev"


def load_config():
    """Load and validate config.yaml."""
    with open(CONFIG_FILE) as f:
        cfg = yaml.safe_load(f)

    # Validate required fields
    for field in ["start_date", "end_date", "markets", "data_dir"]:
        if field not in cfg:
            raise ValueError(f"Missing required config field: {field}")

    cfg["start_date"] = str(cfg["start_date"])
    cfg["end_date"] = str(cfg["end_date"])
    return cfg


def data_dir(cfg):
    """Return the data directory path, creating it if needed."""
    d = Path(__file__).parent / cfg["data_dir"]
    d.mkdir(parents=True, exist_ok=True)
    return d


def orderbook_dir(cfg):
    d = data_dir(cfg) / "orderbook"
    d.mkdir(parents=True, exist_ok=True)
    return d


def trades_dir(cfg):
    d = data_dir(cfg) / "trades"
    d.mkdir(parents=True, exist_ok=True)
    return d


def checkpoint_path(cfg):
    return data_dir(cfg) / "checkpoint.json"


def condition_ids_path(cfg):
    return data_dir(cfg) / "condition_ids.json"


def load_checkpoint(cfg):
    p = checkpoint_path(cfg)
    if p.exists():
        with open(p) as f:
            return json.load(f)
    return {"downloaded_hours": [], "discovered_cids": {}}


def save_checkpoint(cfg, cp):
    with open(checkpoint_path(cfg), "w") as f:
        json.dump(cp, f, indent=2)


def date_range(cfg):
    """Return list of date strings from start_date to end_date inclusive."""
    from datetime import timedelta
    start = datetime.strptime(cfg["start_date"], "%Y-%m-%d")
    end = datetime.strptime(cfg["end_date"], "%Y-%m-%d")
    dates = []
    d = start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def duration_seconds(duration_str):
    """Convert duration string like '5m', '15m', '1h' to seconds."""
    if duration_str.endswith("m"):
        return int(duration_str[:-1]) * 60
    if duration_str.endswith("h"):
        return int(duration_str[:-1]) * 3600
    raise ValueError(f"Unknown duration format: {duration_str}")


def market_slug(asset, duration, start_ts):
    """Build a market slug like btc-updown-5m-1774310400."""
    return f"{asset}-updown-{duration}-{start_ts}"


def discover_condition_ids(cfg, progress=True):
    """Discover condition IDs for all configured markets via Gamma API.

    Returns dict: {slug: condition_id}
    """
    dates = date_range(cfg)
    if not dates:
        return {}

    # Compute the full timestamp range
    start_dt = datetime.strptime(dates[0], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # End date is inclusive — go through 23:59 of that day
    end_dt = datetime.strptime(dates[-1], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_ts = int(end_dt.timestamp()) + 86400  # midnight of next day
    start_ts = int(start_dt.timestamp())

    all_cids = {}
    session = requests.Session()

    for market_cfg in cfg["markets"]:
        asset = market_cfg["asset"]
        duration = market_cfg["duration"]
        step = duration_seconds(duration)

        slugs = []
        for ts in range(start_ts, end_ts, step):
            slugs.append(market_slug(asset, duration, ts))

        if progress:
            print(f"Discovering {asset}-updown-{duration}: {len(slugs)} markets...", flush=True)

        found = 0
        errors = 0
        for i, slug in enumerate(slugs):
            for attempt in range(3):
                try:
                    resp = session.get(GAMMA_API, params={"slug": slug}, timeout=15)
                    if resp.status_code == 404 or not resp.text.strip() or resp.text.strip() == "[]":
                        break
                    resp.raise_for_status()
                    data = resp.json()
                    if not data:
                        break
                    event = data[0] if isinstance(data, list) else data
                    markets = event.get("markets", [])
                    if markets:
                        cid = markets[0].get("conditionId", "")
                        if cid:
                            if not cid.startswith("0x"):
                                cid = f"0x{cid}"
                            all_cids[slug] = cid
                            found += 1
                    break
                except Exception:
                    if attempt < 2:
                        time.sleep(1)
                    else:
                        errors += 1

            # Rate limit: ~5 req/sec
            time.sleep(0.2)

            if progress and (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(slugs)}] found={found} errors={errors}", flush=True)

        if progress:
            print(f"  Done: {found} found, {errors} errors", flush=True)

    return all_cids


def get_archive_file_list():
    """Fetch list of available parquet files from the PMXT archive.

    Returns dict: {filename: url}
    """
    print("Fetching file list from PMXT archive...", flush=True)
    session = requests.Session()
    all_urls = {}
    consecutive_empty = 0

    for page in range(1, 100):
        urls = set()
        for attempt in range(3):
            try:
                url = PMXT_ARCHIVE if page == 1 else f"{PMXT_ARCHIVE}?page={page}"
                resp = session.get(url, timeout=30)
                urls = set(re.findall(
                    r'https://r2\.pmxt\.dev/[a-zA-Z0-9_.-]*parquet', resp.text
                ))
                if urls:
                    break
                time.sleep(1)
            except Exception:
                time.sleep(1)

        new = 0
        for u in urls:
            fname = u.split("/")[-1]
            if fname not in all_urls:
                all_urls[fname] = u
                new += 1

        if new > 0:
            consecutive_empty = 0
            print(f"  Page {page}: {new} new (total: {len(all_urls)})", flush=True)
        else:
            consecutive_empty += 1
            if consecutive_empty >= 4:
                break

    return all_urls
