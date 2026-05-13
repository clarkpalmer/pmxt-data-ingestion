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
PMXT_ENDPOINTS = {
    "v2": {
        "archive": "https://archive.pmxt.dev/Polymarket/v2",
        "download": "https://r2v2.pmxt.dev",
        "url_pattern": r'https://r2v2\.pmxt\.dev/[a-zA-Z0-9_.-]*parquet',
    },
    "v1": {
        "archive": "https://archive.pmxt.dev/Polymarket",
        "download": "https://r2.pmxt.dev",
        "url_pattern": r'https://r2\.pmxt\.dev/[a-zA-Z0-9_.-]*parquet',
    },
}


def load_config():
    """Load and validate config.yaml."""
    with open(CONFIG_FILE) as f:
        cfg = yaml.safe_load(f)

    for field in ["start_date", "end_date", "markets", "data_dir"]:
        if field not in cfg:
            raise ValueError(f"Missing required config field: {field}")

    cfg["start_date"] = str(cfg["start_date"])
    cfg["end_date"] = str(cfg["end_date"])

    version = cfg.get("archive_version", "v2")
    if version not in PMXT_ENDPOINTS:
        raise ValueError(f"Unknown archive_version: {version!r} (use 'v1' or 'v2')")
    cfg["archive_version"] = version

    return cfg


def archive_endpoints(cfg):
    """Return the PMXT endpoint URLs for the configured archive version."""
    return PMXT_ENDPOINTS[cfg["archive_version"]]


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


def parse_date_spec(s):
    """Parse a date spec like '2026-03-23' or '2026-03-23T14' into a UTC datetime.

    Returns (datetime_utc, has_hour).
    """
    s = str(s)
    if "T" in s:
        dt = datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
        return dt, True
    else:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return dt, False


def timestamp_range(cfg):
    """Return (start_ts, end_ts) as UTC unix timestamps for the configured range.

    For hour-level specs: exact hours.
    For day-level specs: full days (00:00 to 24:00).
    """
    start_dt, start_has_hour = parse_date_spec(cfg["start_date"])
    end_dt, end_has_hour = parse_date_spec(cfg["end_date"])

    start_ts = int(start_dt.timestamp())

    if end_has_hour:
        # End hour is inclusive — go through the end of that hour
        end_ts = int(end_dt.timestamp()) + 3600
    else:
        # End date is inclusive — go through end of that day
        end_ts = int(end_dt.timestamp()) + 86400

    return start_ts, end_ts


def date_range(cfg):
    """Return list of date strings from start_date to end_date inclusive."""
    from datetime import timedelta
    start_dt, _ = parse_date_spec(cfg["start_date"])
    end_dt, _ = parse_date_spec(cfg["end_date"])
    # Round to day boundaries
    start_day = start_dt.replace(hour=0, minute=0, second=0)
    end_day = end_dt.replace(hour=0, minute=0, second=0)
    dates = []
    d = start_day
    while d <= end_day:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


def hour_range(cfg):
    """Return list of (date_str, hour_str) tuples for the configured range.

    Useful for matching against archive filenames like '2026-03-23T14'.
    """
    from datetime import timedelta
    start_dt, start_has_hour = parse_date_spec(cfg["start_date"])
    end_dt, end_has_hour = parse_date_spec(cfg["end_date"])

    if not start_has_hour:
        start_dt = start_dt.replace(hour=0)
    if not end_has_hour:
        end_dt = end_dt.replace(hour=23)

    hours = []
    dt = start_dt
    while dt <= end_dt:
        hours.append((dt.strftime("%Y-%m-%d"), f"{dt.hour:02d}"))
        dt += timedelta(hours=1)
    return hours


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
    start_ts, end_ts = timestamp_range(cfg)
    if start_ts >= end_ts:
        return {}

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


def get_archive_file_list(cfg):
    """Fetch list of available parquet files from the PMXT archive.

    Returns dict: {filename: url}
    """
    ep = archive_endpoints(cfg)
    archive_url = ep["archive"]
    url_pattern = ep["url_pattern"]
    version = cfg["archive_version"]

    print(f"Fetching file list from PMXT archive ({version})...", flush=True)
    session = requests.Session()
    all_urls = {}
    consecutive_empty = 0

    for page in range(1, 100):
        urls = set()
        for attempt in range(3):
            try:
                url = archive_url if page == 1 else f"{archive_url}?page={page}"
                resp = session.get(url, timeout=30)
                urls = set(re.findall(url_pattern, resp.text))
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
