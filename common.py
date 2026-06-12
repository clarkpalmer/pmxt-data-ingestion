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
# Telonex serves the full Polymarket markets dataset (1.28M+ markets: slugs,
# condition IDs, questions, outcomes, resolutions, tags, data-coverage dates)
# as a single parquet at a public endpoint — free, no API key.
TELONEX_MARKETS_URL = "https://api.telonex.io/v1/datasets/polymarket/markets"
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


MARKET_SELECTORS = ["markets", "slugs", "condition_ids", "event_ids", "tags"]


def load_config():
    """Load and validate config.yaml."""
    with open(CONFIG_FILE) as f:
        cfg = yaml.safe_load(f)

    for field in ["start_date", "end_date", "data_dir"]:
        if field not in cfg:
            raise ValueError(f"Missing required config field: {field}")

    has_selector = any(cfg.get(s) for s in MARKET_SELECTORS)
    if not has_selector:
        raise ValueError(
            f"At least one market selector required: {', '.join(MARKET_SELECTORS)}"
        )

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


# ---------------------------------------------------------------------------
# Markets snapshot (Telonex)
# ---------------------------------------------------------------------------

def _raw_config():
    """Load config.yaml without validation (snapshot helpers must work even
    when no market selectors are configured yet)."""
    try:
        with open(CONFIG_FILE) as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}


def snapshot_dest():
    """Default path the markets snapshot is downloaded to (inside data_dir)."""
    cfg = _raw_config()
    d = Path(__file__).parent / cfg.get("data_dir", "data")
    d.mkdir(parents=True, exist_ok=True)
    return d / "polymarket_markets.parquet"


def find_markets_snapshot():
    """Locate an existing markets snapshot parquet.

    Order: explicit `markets_snapshot:` config override, then the default
    download location in data_dir, then a couple of conventional spots.
    Returns a Path or None.
    """
    cfg = _raw_config()
    candidates = []
    override = cfg.get("markets_snapshot")
    if override:
        candidates.append(Path(override).expanduser())
    candidates.append(
        Path(__file__).parent / cfg.get("data_dir", "data") / "polymarket_markets.parquet"
    )
    candidates += [
        Path(__file__).parent / "polymarket_markets.parquet",
        Path.home() / "Downloads" / "polymarket_markets.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


def snapshot_status():
    """Status of the local snapshot: path, size, age, row count. None if absent."""
    p = find_markets_snapshot()
    if p is None:
        return None
    st = p.stat()
    info = {
        "path": str(p),
        "size_mb": round(st.st_size / 1e6, 1),
        "age_seconds": int(time.time() - st.st_mtime),
    }
    try:
        import pyarrow.parquet as pq
        info["rows"] = pq.read_metadata(p).num_rows
    except Exception:
        info["rows"] = None
    return info


def download_markets_snapshot(dest=None, progress=None):
    """Stream-download the markets dataset from Telonex (free, no API key).

    progress: optional callback(bytes_done, bytes_total_or_None), called per chunk.
    Writes to a temp file and renames atomically, so a torn download never
    replaces a good snapshot. Returns the destination Path.
    """
    import secrets
    dest = Path(dest) if dest else snapshot_dest()
    dest.parent.mkdir(parents=True, exist_ok=True)
    # Unique temp name: concurrent downloads (CLI + dashboard) must not
    # interleave writes into the same file.
    tmp = dest.parent / f"{dest.name}.{secrets.token_hex(8)}.tmp"

    try:
        with requests.get(
            TELONEX_MARKETS_URL, stream=True, timeout=120, allow_redirects=True
        ) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("Content-Length") or 0) or None
            done = 0
            with open(tmp, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    done += len(chunk)
                    if progress:
                        progress(done, total)

        # A truncated body would poison every consumer — verify parquet magic.
        with open(tmp, "rb") as f:
            head = f.read(4)
            f.seek(-4, 2)
            tail = f.read(4)
        if head != b"PAR1" or tail != b"PAR1":
            raise RuntimeError("Downloaded snapshot is not a valid parquet file")

        tmp.replace(dest)
        return dest
    finally:
        tmp.unlink(missing_ok=True)


def snapshot_lookup_slugs(snapshot, slugs):
    """Bulk slug -> condition_id resolution from the local snapshot.

    One DuckDB join instead of one Gamma API call per slug. Returns a dict
    covering only the slugs present in the snapshot; resolve the rest via
    the API.
    """
    if not slugs:
        return {}
    import duckdb
    con = duckdb.connect()
    try:
        con.execute("CREATE TEMP TABLE wanted(slug VARCHAR)")
        con.executemany("INSERT INTO wanted VALUES (?)", [(s,) for s in slugs])
        rows = con.execute(
            f"""SELECT m.slug, m.market_id
                FROM read_parquet('{snapshot}') m
                JOIN wanted w ON m.slug = w.slug
                WHERE m.market_id IS NOT NULL AND m.market_id != ''"""
        ).fetchall()
    finally:
        con.close()
    return {s: (c if c.startswith("0x") else f"0x{c}") for s, c in rows}


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


def _gamma_lookup_slug(session, slug):
    """Look up a single slug via Gamma API. Returns condition_id or None."""
    for attempt in range(3):
        try:
            resp = session.get(GAMMA_API, params={"slug": slug}, timeout=15)
            if resp.status_code == 404 or not resp.text.strip() or resp.text.strip() == "[]":
                return None
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return None
            event = data[0] if isinstance(data, list) else data
            markets = event.get("markets", [])
            if markets:
                cid = markets[0].get("conditionId", "")
                if cid:
                    return cid if cid.startswith("0x") else f"0x{cid}"
            return None
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return None


def _gamma_lookup_event(session, event_id):
    """Look up an event ID via Gamma API. Returns list of (slug, condition_id)."""
    for attempt in range(3):
        try:
            resp = session.get(GAMMA_API, params={"id": event_id}, timeout=15)
            if resp.status_code == 404 or not resp.text.strip() or resp.text.strip() == "[]":
                return []
            resp.raise_for_status()
            data = resp.json()
            if not data:
                return []
            event = data[0] if isinstance(data, list) else data
            results = []
            for m in event.get("markets", []):
                cid = m.get("conditionId", "")
                slug = m.get("slug", event.get("slug", f"event-{event_id}"))
                if cid:
                    cid = cid if cid.startswith("0x") else f"0x{cid}"
                    results.append((slug, cid))
            return results
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return []


def _gamma_search_tag(session, tag, limit=100):
    """Search Gamma API by tag. Returns list of (slug, condition_id)."""
    results = []
    offset = 0
    while True:
        for attempt in range(3):
            try:
                resp = session.get(
                    GAMMA_API,
                    params={"tag": tag, "limit": limit, "offset": offset},
                    timeout=30,
                )
                resp.raise_for_status()
                data = resp.json()
                if not data:
                    return results
                for event in (data if isinstance(data, list) else [data]):
                    for m in event.get("markets", []):
                        cid = m.get("conditionId", "")
                        slug = m.get("slug", event.get("slug", ""))
                        if cid:
                            cid = cid if cid.startswith("0x") else f"0x{cid}"
                            results.append((slug, cid))
                if len(data) < limit:
                    return results
                offset += limit
                break
            except Exception:
                if attempt < 2:
                    time.sleep(1)
                else:
                    return results
    return results


def _resolve_updown_markets(cfg, session, progress=True):
    """Resolve the `markets` config (asset+duration updown pattern)."""
    markets_cfg = cfg.get("markets", [])
    if not markets_cfg:
        return {}

    start_ts, end_ts = timestamp_range(cfg)
    if start_ts >= end_ts:
        return {}

    snapshot = find_markets_snapshot()
    all_cids = {}
    for market_cfg in markets_cfg:
        asset = market_cfg["asset"]
        duration = market_cfg["duration"]
        step = duration_seconds(duration)

        slugs = [market_slug(asset, duration, ts) for ts in range(start_ts, end_ts, step)]

        if progress:
            print(f"Discovering {asset}-updown-{duration}: {len(slugs)} markets...", flush=True)

        # Local snapshot first (one bulk join, instant); Gamma only for misses
        # (markets newer than the snapshot, or that never existed).
        snap_found = 0
        if snapshot:
            hits = snapshot_lookup_slugs(snapshot, slugs)
            all_cids.update(hits)
            snap_found = len(hits)
            slugs = [s for s in slugs if s not in hits]
            if progress and snap_found:
                print(f"  {snap_found} resolved from local snapshot, "
                      f"{len(slugs)} left for Gamma API", flush=True)

        found = errors = 0
        for i, slug in enumerate(slugs):
            cid = _gamma_lookup_slug(session, slug)
            if cid:
                all_cids[slug] = cid
                found += 1
            time.sleep(0.2)
            if progress and (i + 1) % 100 == 0:
                print(f"  [{i+1}/{len(slugs)}] found={found} errors={errors}", flush=True)

        if progress:
            print(f"  Done: {snap_found + found} found "
                  f"({snap_found} snapshot, {found} Gamma), {errors} errors", flush=True)

    return all_cids


def resolve_all_condition_ids(cfg, progress=True):
    """Resolve condition IDs from all configured selectors.

    Supports: markets (updown), slugs, condition_ids, event_ids, tags.
    Returns dict: {label: condition_id}
    """
    all_cids = {}
    session = requests.Session()

    # 1. Direct condition IDs — no API call needed
    for cid in cfg.get("condition_ids", []):
        cid = str(cid)
        if not cid.startswith("0x"):
            cid = f"0x{cid}"
        all_cids[cid] = cid
    if cfg.get("condition_ids") and progress:
        print(f"Direct condition IDs: {len(cfg['condition_ids'])}", flush=True)

    # 2. Updown markets (asset+duration pattern)
    all_cids.update(_resolve_updown_markets(cfg, session, progress))

    # 3. Arbitrary slugs — snapshot first, Gamma for the misses
    slugs_cfg = list(cfg.get("slugs", []))
    if slugs_cfg:
        if progress:
            print(f"Resolving {len(slugs_cfg)} slug(s)...", flush=True)
        found = 0
        snapshot = find_markets_snapshot()
        if snapshot:
            hits = snapshot_lookup_slugs(snapshot, slugs_cfg)
            all_cids.update(hits)
            found += len(hits)
            slugs_cfg = [s for s in slugs_cfg if s not in hits]
        for slug in slugs_cfg:
            cid = _gamma_lookup_slug(session, slug)
            if cid:
                all_cids[slug] = cid
                found += 1
            time.sleep(0.2)
        if progress:
            print(f"  Done: {found} found", flush=True)

    # 4. Event IDs — all markets under each event
    for event_id in cfg.get("event_ids", []):
        if progress:
            print(f"Resolving event {event_id}...", flush=True)
        pairs = _gamma_lookup_event(session, str(event_id))
        for slug, cid in pairs:
            all_cids[slug] = cid
        if progress:
            print(f"  Found {len(pairs)} market(s)", flush=True)
        time.sleep(0.2)

    # 5. Tags — search by tag
    for tag in cfg.get("tags", []):
        if progress:
            print(f"Searching tag '{tag}'...", flush=True)
        pairs = _gamma_search_tag(session, tag)
        for slug, cid in pairs:
            all_cids[slug] = cid
        if progress:
            print(f"  Found {len(pairs)} market(s)", flush=True)
        time.sleep(0.2)

    return all_cids


def discover_condition_ids(cfg, progress=True):
    """Discover condition IDs for all configured markets.

    Legacy wrapper — calls resolve_all_condition_ids.
    """
    return resolve_all_condition_ids(cfg, progress)


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
