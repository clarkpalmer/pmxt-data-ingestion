#!/usr/bin/env python3
"""Download PMXT orderbook data, filter to configured markets, checkpoint progress.

Downloads hourly parquet files from the PMXT archive, filters them to only
include rows matching your configured markets (by condition ID), and saves
filtered daily parquet files. Supports resuming via checkpoint.

Usage:
    python download.py              # Download and filter
    python download.py --status     # Show download progress
    python download.py --discover   # Only discover condition IDs (no download)
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

import duckdb

from common import (
    load_config, load_checkpoint, save_checkpoint, data_dir, orderbook_dir,
    condition_ids_path, date_range, hour_range, timestamp_range,
    discover_condition_ids, get_archive_file_list,
)


def download_file(url, dest, connections=4):
    """Download a file. Uses aria2c if available, otherwise requests."""
    if shutil.which("aria2c"):
        cmd = [
            "aria2c", "-x", str(connections), "-s", str(connections), "-k", "1M",
            "--file-allocation=none", "--console-log-level=warn",
            "-d", str(dest.parent), "-o", dest.name,
            "--allow-overwrite=true", url,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        return result.returncode == 0 and dest.exists()
    else:
        # Fallback to requests
        import requests
        try:
            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            return dest.exists()
        except Exception as e:
            print(f"Download error: {e}")
            return False


def filter_parquet(filepath, cid_set, out_file):
    """Filter a parquet file to only rows matching our condition IDs.

    Returns number of rows written.
    """
    con = duckdb.connect()
    con.execute("CREATE TEMP TABLE cids (cid VARCHAR)")
    con.executemany("INSERT INTO cids VALUES (?)", [(c,) for c in cid_set])

    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet('{filepath}')
            WHERE market_id IN (SELECT cid FROM cids)
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    con.close()

    if out_file.exists() and out_file.stat().st_size > 0:
        con2 = duckdb.connect()
        n = con2.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out_file}')"
        ).fetchone()[0]
        con2.close()
        return n
    return 0


def merge_hourly_to_daily(hourly_dir, date_str, out_dir):
    """Merge hourly chunk files into a single daily parquet."""
    hourly_files = sorted(hourly_dir.glob(f"chunk_{date_str}_T*.parquet"))
    if not hourly_files:
        return 0

    out_file = out_dir / f"orderbook_{date_str}.parquet"
    con = duckdb.connect()
    file_list = ", ".join(f"'{f}'" for f in hourly_files)
    con.execute(f"""
        COPY (
            SELECT * FROM read_parquet([{file_list}])
            ORDER BY timestamp_received
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out_file}')"
    ).fetchone()[0]
    con.close()

    # Clean up hourly chunks
    for f in hourly_files:
        f.unlink()

    size_mb = out_file.stat().st_size / 1e6
    print(f"  Merged {date_str}: {len(hourly_files)} hours -> {n:,} rows ({size_mb:.1f} MB)")
    return n


def show_status(cfg):
    cp = load_checkpoint(cfg)
    downloaded = set(cp.get("downloaded_hours", []))
    cids = cp.get("discovered_cids", {})
    target_hours = hour_range(cfg)
    dates = date_range(cfg)
    start_ts, end_ts = timestamp_range(cfg)

    print(f"Config: {len(cfg['markets'])} market type(s)")
    print(f"Range: {cfg['start_date']} to {cfg['end_date']} ({len(target_hours)} hours)")
    print(f"Timestamps: {start_ts} to {end_ts}")
    print(f"Condition IDs discovered: {len(cids)}")
    print(f"Archive hours downloaded: {len(downloaded)}")

    # Coverage by day
    hours_by_day = defaultdict(set)
    for f in downloaded:
        m = re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', f)
        if m:
            hours_by_day[m.group(1)].add(int(m.group(2)))

    # Target hours by day
    target_by_day = defaultdict(set)
    for d, h in target_hours:
        target_by_day[d].add(int(h))

    print(f"\nCoverage:")
    ob_dir = orderbook_dir(cfg)
    for day in dates:
        target_h = sorted(target_by_day.get(day, set()))
        done_h = sorted(hours_by_day.get(day, set()) & set(target_h))
        n_target = len(target_h)
        status = "COMPLETE" if len(done_h) == n_target else f"{len(done_h)}/{n_target}"
        day_file = ob_dir / f"orderbook_{day}.parquet"
        size = ""
        if day_file.exists():
            mb = day_file.stat().st_size / 1e6
            con = duckdb.connect()
            n = con.execute(
                f"SELECT COUNT(*) FROM read_parquet('{day_file}')"
            ).fetchone()[0]
            con.close()
            size = f"  ({n:,} rows, {mb:.1f} MB)"
        print(f"  {day}: {status} (hours {target_h[0]:02d}-{target_h[-1]:02d}){size}")


def main():
    parser = argparse.ArgumentParser(description="Download PMXT orderbook data")
    parser.add_argument("--status", action="store_true", help="Show download progress")
    parser.add_argument("--discover", action="store_true", help="Only discover condition IDs")
    args = parser.parse_args()

    cfg = load_config()

    if args.status:
        show_status(cfg)
        return

    # Step 1: Discover condition IDs if we don't have them yet
    cp = load_checkpoint(cfg)
    cids = cp.get("discovered_cids", {})

    if not cids or args.discover:
        print("=" * 60)
        print("Step 1: Discovering condition IDs via Gamma API")
        print("=" * 60)
        cids = discover_condition_ids(cfg)
        cp["discovered_cids"] = cids
        save_checkpoint(cfg, cp)

        # Also save a standalone file for reference
        with open(condition_ids_path(cfg), "w") as f:
            json.dump(cids, f, indent=2)
        print(f"\nSaved {len(cids)} condition IDs")

        if args.discover:
            return

    cid_set = set(cids.values())
    print(f"\nUsing {len(cid_set)} condition IDs across {len(cfg['markets'])} market type(s)")

    # Step 2: Get archive file list
    print("\n" + "=" * 60)
    print("Step 2: Fetching archive file list")
    print("=" * 60)
    all_urls = get_archive_file_list()

    # Filter to our hour range
    target_hours_set = set(hour_range(cfg))  # set of (date_str, hour_str)
    downloaded = set(cp.get("downloaded_hours", []))

    to_process = []
    for fname, url in sorted(all_urls.items()):
        m = re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', fname)
        if not m:
            continue
        if (m.group(1), m.group(2)) not in target_hours_set:
            continue
        if fname in downloaded:
            continue
        to_process.append((fname, url))

    in_range = sum(
        1 for f in all_urls
        if re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', f)
        and (re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', f).group(1),
             re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', f).group(2)) in target_hours_set
    )
    print(f"\nArchive files in range: {in_range} ({len(target_hours_set)} hours requested)")
    print(f"Already downloaded: {len(downloaded)}")
    print(f"Remaining: {len(to_process)}")

    if not to_process:
        print("Nothing to download!")
        return

    # Step 3: Download, filter, checkpoint
    print("\n" + "=" * 60)
    print("Step 3: Downloading and filtering")
    print("=" * 60)

    dl_cfg = cfg.get("download", {})
    temp_dir = Path(dl_cfg.get("temp_dir", "/tmp/pmxt_ingestion"))
    connections = dl_cfg.get("connections", 4)
    hourly_dir = temp_dir / "hourly"
    temp_dir.mkdir(parents=True, exist_ok=True)
    hourly_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    total_rows = 0
    days_touched = set()

    for i, (fname, url) in enumerate(to_process):
        m = re.search(r'(\d{4}-\d{2}-\d{2})T(\d{2})', fname)
        date_str = m.group(1)
        hour = m.group(2)
        days_touched.add(date_str)

        dest = temp_dir / fname
        print(f"[{i+1}/{len(to_process)}] {fname} ...", end=" ", flush=True)

        ok = download_file(url, dest, connections)
        if not ok:
            print("DOWNLOAD FAILED", flush=True)
            continue

        size_mb = dest.stat().st_size / 1e6

        # Filter to our markets
        chunk_file = hourly_dir / f"chunk_{date_str}_T{hour}.parquet"
        try:
            n = filter_parquet(dest, cid_set, chunk_file)
            total_rows += n
            print(f"{size_mb:.0f} MB -> {n:,} rows", flush=True)
        except Exception as e:
            print(f"FILTER ERROR: {e}", flush=True)
            dest.unlink(missing_ok=True)
            continue

        # Clean up raw file immediately
        dest.unlink(missing_ok=True)

        # Update checkpoint
        downloaded.add(fname)
        cp["downloaded_hours"] = list(downloaded)
        save_checkpoint(cfg, cp)

    # Step 4: Merge hourly chunks into daily files
    print("\n" + "=" * 60)
    print("Step 4: Merging into daily files")
    print("=" * 60)

    ob_dir = orderbook_dir(cfg)
    for date_str in sorted(days_touched):
        merge_hourly_to_daily(hourly_dir, date_str, ob_dir)

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed/60:.1f} minutes")
    print(f"Total rows: {total_rows:,}")


if __name__ == "__main__":
    main()
