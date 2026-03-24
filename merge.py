#!/usr/bin/env python3
"""Merge downloaded orderbook files by day or into a single file.

Usage:
    python merge.py                  # Merge all daily files into one
    python merge.py --by-day         # Re-merge hourly chunks into daily files
    python merge.py --output all.parquet  # Custom output filename
"""

import argparse
from pathlib import Path

import duckdb

from common import load_config, orderbook_dir


def merge_all(cfg, output_name="orderbook_all.parquet"):
    """Merge all daily orderbook files into a single parquet file."""
    ob_dir = orderbook_dir(cfg)
    daily_files = sorted(ob_dir.glob("orderbook_*.parquet"))

    # Exclude the output file itself if it exists
    out_file = ob_dir / output_name
    daily_files = [f for f in daily_files if f.name != output_name]

    if not daily_files:
        print("No daily files to merge.")
        return

    print(f"Merging {len(daily_files)} daily files:")
    for f in daily_files:
        mb = f.stat().st_size / 1e6
        print(f"  {f.name} ({mb:.1f} MB)")

    con = duckdb.connect()
    file_list = ", ".join(f"'{f}'" for f in daily_files)
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

    mb = out_file.stat().st_size / 1e6
    print(f"\nMerged: {out_file.name} ({n:,} rows, {mb:.1f} MB)")


def main():
    parser = argparse.ArgumentParser(description="Merge orderbook parquet files")
    parser.add_argument("--output", default="orderbook_all.parquet",
                        help="Output filename (default: orderbook_all.parquet)")
    args = parser.parse_args()

    cfg = load_config()
    merge_all(cfg, args.output)


if __name__ == "__main__":
    main()
