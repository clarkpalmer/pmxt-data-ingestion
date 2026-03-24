#!/usr/bin/env python3
"""Fetch trade data, resolutions, and strike prices for downloaded markets.

For each market in your condition_ids.json, this script:
1. Fetches all trades from the Polymarket Data API
2. Fetches the resolution (Up/Down) and strike price from the Gamma API
3. Saves trades.parquet and resolutions.parquet

Usage:
    python enrich.py                # Fetch everything
    python enrich.py --trades       # Only fetch trades
    python enrich.py --resolutions  # Only fetch resolutions
    python enrich.py --status       # Show progress
"""

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from common import (
    load_config, data_dir, trades_dir, condition_ids_path,
    load_checkpoint, save_checkpoint,
    GAMMA_API, DATA_API,
)

TRADE_WORKERS = 20
TRADE_PAGE_SIZE = 500
MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

def fetch_market_trades(cid, session):
    """Fetch all trades for a single condition ID. Returns list of dicts."""
    all_trades = []
    offset = 0

    for _ in range(100):
        for attempt in range(MAX_RETRIES):
            try:
                resp = session.get(DATA_API, params={
                    "market": cid, "limit": TRADE_PAGE_SIZE, "offset": offset,
                }, timeout=15)

                if resp.status_code == 429:
                    time.sleep(1 + attempt)
                    continue
                if resp.status_code != 200:
                    return all_trades

                data = resp.json()
                if not data:
                    return all_trades

                all_trades.extend(data)
                offset += len(data)

                if len(data) < TRADE_PAGE_SIZE:
                    return all_trades
                break

            except Exception:
                time.sleep(0.5 + attempt)
        else:
            return all_trades

    return all_trades


def download_trades(cfg, slug_to_cid):
    """Download trades for all markets."""
    cp = load_checkpoint(cfg)
    done_cids = set(cp.get("trades_done", []))
    remaining = {s: c for s, c in slug_to_cid.items() if c not in done_cids}

    print(f"Trades: {len(slug_to_cid)} total, {len(done_cids)} done, {len(remaining)} remaining")

    if not remaining:
        print("All trades already downloaded.")
        return

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)

    t_dir = trades_dir(cfg)
    all_rows = []
    total = 0
    t0 = time.time()

    # Process in batches
    items = list(remaining.items())
    BATCH = 200

    for batch_start in range(0, len(items), BATCH):
        batch = items[batch_start:batch_start + BATCH]
        batch_rows = []

        with ThreadPoolExecutor(max_workers=TRADE_WORKERS) as pool:
            futures = {
                pool.submit(fetch_market_trades, cid, session): (slug, cid)
                for slug, cid in batch
            }
            for future in as_completed(futures):
                slug, cid = futures[future]
                try:
                    trades = future.result()
                    for t in trades:
                        batch_rows.append({
                            "condition_id": cid,
                            "slug": slug,
                            "asset_id": str(t.get("asset", "")),
                            "side": t.get("side", ""),
                            "price": float(t.get("price", 0)),
                            "size": float(t.get("size", 0)),
                            "timestamp": int(t.get("timestamp", 0)),
                            "outcome": t.get("outcome", ""),
                            "tx_hash": t.get("transactionHash", ""),
                        })
                    done_cids.add(cid)
                except Exception:
                    pass

        all_rows.extend(batch_rows)
        total += len(batch_rows)

        cp["trades_done"] = list(done_cids)
        save_checkpoint(cfg, cp)

        elapsed = time.time() - t0
        print(f"  Batch {batch_start // BATCH + 1}: {total:,} trades, "
              f"{len(done_cids)}/{len(slug_to_cid)} markets", flush=True)

    if all_rows:
        # Build arrow table and write
        table = pa.table({
            "condition_id": [r["condition_id"] for r in all_rows],
            "slug": [r["slug"] for r in all_rows],
            "asset_id": [r["asset_id"] for r in all_rows],
            "side": [r["side"] for r in all_rows],
            "price": [r["price"] for r in all_rows],
            "size": [r["size"] for r in all_rows],
            "timestamp": [r["timestamp"] for r in all_rows],
            "outcome": [r["outcome"] for r in all_rows],
            "tx_hash": [r["tx_hash"] for r in all_rows],
        })

        out_file = t_dir / "trades.parquet"
        # Append to existing if present
        if out_file.exists():
            existing = pq.read_table(out_file)
            table = pa.concat_tables([existing, table])

        pq.write_table(table, out_file)
        mb = out_file.stat().st_size / 1e6
        print(f"\nSaved {len(table):,} trades to {out_file} ({mb:.1f} MB)")


# ---------------------------------------------------------------------------
# Resolutions & strike prices
# ---------------------------------------------------------------------------

def fetch_resolution(slug, session):
    """Fetch resolution + strike price for a single market from Gamma API."""
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
            if not markets:
                return None
            return markets[0]
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return None


def download_resolutions(cfg, slug_to_cid):
    """Download resolutions and strike prices for all markets."""
    cp = load_checkpoint(cfg)
    done_slugs = set(cp.get("resolutions_done", []))
    remaining = {s: c for s, c in slug_to_cid.items() if s not in done_slugs}

    print(f"Resolutions: {len(slug_to_cid)} total, {len(done_slugs)} done, {len(remaining)} remaining")

    if not remaining:
        print("All resolutions already downloaded.")
        return

    session = requests.Session()
    rows = []
    resolved = 0
    unresolved = 0
    errors = 0

    for i, (slug, cid) in enumerate(remaining.items()):
        market = fetch_resolution(slug, session)
        if market is None:
            errors += 1
            done_slugs.add(slug)
            continue

        # Parse timestamp from slug
        try:
            ts = int(slug.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            ts = 0

        # Parse the slug to get asset and duration
        parts = slug.split("-")
        asset = parts[0] if parts else ""
        duration = parts[2] if len(parts) > 2 else ""

        # Resolution
        outcome_prices = json.loads(market.get("outcomePrices", "[]"))
        outcomes = json.loads(market.get("outcomes", "[]"))
        outcome = None
        for j, price in enumerate(outcome_prices):
            if float(price) >= 0.99:
                outcome = outcomes[j]
                break

        # Strike price from eventMetadata
        metadata = market.get("eventMetadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except Exception:
                metadata = {}

        strike_price = metadata.get("priceToBeat")
        final_price = metadata.get("finalPrice")

        # Time info
        start_time = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat() if ts else ""
        duration_secs = {"5m": 300, "15m": 900, "1h": 3600}.get(duration, 300)
        end_time = datetime.fromtimestamp(ts + duration_secs, tz=timezone.utc).isoformat() if ts else ""

        row = {
            "slug": slug,
            "condition_id": cid,
            "asset": asset,
            "duration": duration,
            "start_time": start_time,
            "end_time": end_time,
            "outcome": outcome or "",
            "resolved": outcome is not None,
            "strike_price": float(strike_price) if strike_price is not None else None,
            "final_price": float(final_price) if final_price is not None else None,
            "volume": float(market.get("volume", 0)),
            "question": market.get("question", ""),
        }
        rows.append(row)

        if outcome:
            resolved += 1
        else:
            unresolved += 1

        done_slugs.add(slug)

        if (i + 1) % 50 == 0:
            print(f"  [{i+1}/{len(remaining)}] resolved={resolved} "
                  f"unresolved={unresolved} errors={errors}", flush=True)
            cp["resolutions_done"] = list(done_slugs)
            save_checkpoint(cfg, cp)

        time.sleep(0.2)

    cp["resolutions_done"] = list(done_slugs)
    save_checkpoint(cfg, cp)

    if rows:
        out_file = data_dir(cfg) / "resolutions.parquet"

        # Append to existing
        if out_file.exists():
            existing = pq.read_table(out_file).to_pydict()
            for key in rows[0]:
                if key in existing:
                    existing[key].extend(r[key] for r in rows)
                else:
                    existing[key] = [r[key] for r in rows]
            table = pa.table(existing)
        else:
            table = pa.table({k: [r[k] for r in rows] for k in rows[0]})

        pq.write_table(table, out_file)
        print(f"\nSaved {len(table):,} resolutions to {out_file}")
        print(f"  Resolved: {resolved}, Unresolved: {unresolved}, Errors: {errors}")


def show_status(cfg):
    cp = load_checkpoint(cfg)
    cids = cp.get("discovered_cids", {})
    trades_done = len(cp.get("trades_done", []))
    resolutions_done = len(cp.get("resolutions_done", []))

    print(f"Markets: {len(cids)}")
    print(f"Trades downloaded: {trades_done}/{len(cids)}")
    print(f"Resolutions downloaded: {resolutions_done}/{len(cids)}")

    t_file = trades_dir(cfg) / "trades.parquet"
    if t_file.exists():
        mb = t_file.stat().st_size / 1e6
        n = pq.read_metadata(t_file).num_rows
        print(f"\ntrades.parquet: {n:,} rows ({mb:.1f} MB)")

    r_file = data_dir(cfg) / "resolutions.parquet"
    if r_file.exists():
        mb = r_file.stat().st_size / 1e6
        n = pq.read_metadata(r_file).num_rows
        print(f"resolutions.parquet: {n:,} rows ({mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(description="Fetch trades, resolutions, and strike prices")
    parser.add_argument("--trades", action="store_true", help="Only fetch trades")
    parser.add_argument("--resolutions", action="store_true", help="Only fetch resolutions")
    parser.add_argument("--status", action="store_true", help="Show progress")
    args = parser.parse_args()

    cfg = load_config()

    if args.status:
        show_status(cfg)
        return

    # Load condition IDs
    cid_file = condition_ids_path(cfg)
    if not cid_file.exists():
        cp = load_checkpoint(cfg)
        cids = cp.get("discovered_cids", {})
        if not cids:
            print("No condition IDs found. Run download.py first (or download.py --discover).")
            return
    else:
        with open(cid_file) as f:
            cids = json.load(f)

    print(f"Markets: {len(cids)}")

    do_both = not args.trades and not args.resolutions

    if args.trades or do_both:
        print("\n" + "=" * 60)
        print("Fetching trades")
        print("=" * 60)
        download_trades(cfg, cids)

    if args.resolutions or do_both:
        print("\n" + "=" * 60)
        print("Fetching resolutions & strike prices")
        print("=" * 60)
        download_resolutions(cfg, cids)


if __name__ == "__main__":
    main()
