#!/usr/bin/env python3
"""Report on downloaded data: markets, rows, dates, coverage, data quality.

Usage:
    python report.py              # Full report
    python report.py --summary    # Short summary only
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from common import load_config, data_dir, orderbook_dir, trades_dir, condition_ids_path


def report_orderbook(cfg):
    """Report on orderbook data."""
    ob_dir = orderbook_dir(cfg)
    files = sorted(ob_dir.glob("orderbook_*.parquet"))
    merged = [f for f in files if "all" not in f.name]

    if not merged:
        print("  No orderbook files found.")
        return

    print(f"  Files: {len(merged)}")

    con = duckdb.connect()
    total_rows = 0
    total_size = 0

    for f in merged:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        mb = f.stat().st_size / 1e6
        total_rows += n
        total_size += mb

        # Count by update_type
        types = con.execute(f"""
            SELECT update_type, COUNT(*) as cnt
            FROM read_parquet('{f}')
            GROUP BY update_type
        """).fetchdf()
        type_str = ", ".join(f"{r['update_type']}={r['cnt']:,}" for _, r in types.iterrows())
        print(f"  {f.name}: {n:,} rows ({mb:.1f} MB) [{type_str}]")

    print(f"  Total: {total_rows:,} rows ({total_size:.1f} MB)")

    # Count unique markets
    if merged:
        file_list = ", ".join(f"'{f}'" for f in merged)
        n_markets = con.execute(f"""
            SELECT COUNT(DISTINCT market_id)
            FROM read_parquet([{file_list}])
        """).fetchone()[0]
        print(f"  Unique markets (condition IDs): {n_markets}")

    con.close()


def report_trades(cfg):
    """Report on trade data."""
    t_file = trades_dir(cfg) / "trades.parquet"
    if not t_file.exists():
        print("  No trades file found. Run: python enrich.py --trades")
        return

    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{t_file}')").fetchone()[0]
    mb = t_file.stat().st_size / 1e6
    print(f"  File: trades.parquet ({n:,} rows, {mb:.1f} MB)")

    # Markets with trades
    n_markets = con.execute(f"""
        SELECT COUNT(DISTINCT condition_id) FROM read_parquet('{t_file}')
    """).fetchone()[0]
    print(f"  Markets with trades: {n_markets}")

    # Trade stats
    stats = con.execute(f"""
        SELECT
            COUNT(*) as n_trades,
            SUM(price * size) as total_volume,
            MIN(timestamp) as first_ts,
            MAX(timestamp) as last_ts
        FROM read_parquet('{t_file}')
    """).fetchone()
    if stats[2]:
        first = datetime.fromtimestamp(stats[2], tz=timezone.utc)
        last = datetime.fromtimestamp(stats[3], tz=timezone.utc)
        print(f"  Time range: {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC")
    if stats[1]:
        print(f"  Total volume: ${stats[1]:,.0f}")

    # By side
    sides = con.execute(f"""
        SELECT side, COUNT(*) as cnt, SUM(size) as total_size
        FROM read_parquet('{t_file}')
        GROUP BY side
    """).fetchdf()
    for _, r in sides.iterrows():
        print(f"  {r['side']}: {r['cnt']:,} trades, {r['total_size']:,.0f} shares")

    con.close()


def report_resolutions(cfg):
    """Report on resolution data."""
    r_file = data_dir(cfg) / "resolutions.parquet"
    if not r_file.exists():
        print("  No resolutions file found. Run: python enrich.py --resolutions")
        return

    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{r_file}')").fetchone()[0]
    print(f"  File: resolutions.parquet ({n:,} rows)")

    # Resolution stats
    resolved = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{r_file}') WHERE resolved = true
    """).fetchone()[0]
    print(f"  Resolved: {resolved}/{n}")

    # Outcome distribution
    outcomes = con.execute(f"""
        SELECT outcome, COUNT(*) as cnt
        FROM read_parquet('{r_file}')
        WHERE resolved = true
        GROUP BY outcome
    """).fetchdf()
    for _, r in outcomes.iterrows():
        pct = r['cnt'] / resolved * 100 if resolved > 0 else 0
        print(f"  {r['outcome']}: {r['cnt']:,} ({pct:.1f}%)")

    # Strike price stats
    has_strike = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{r_file}') WHERE strike_price IS NOT NULL
    """).fetchone()[0]
    if has_strike:
        stats = con.execute(f"""
            SELECT
                MIN(strike_price) as min_sp,
                MAX(strike_price) as max_sp,
                AVG(strike_price) as avg_sp
            FROM read_parquet('{r_file}')
            WHERE strike_price IS NOT NULL
        """).fetchone()
        print(f"  Strike prices: {has_strike:,} markets")
        print(f"    Range: ${stats[0]:,.2f} - ${stats[1]:,.2f}")
        print(f"    Mean:  ${stats[2]:,.2f}")

    # By asset
    cols = [desc[0] for desc in con.execute(f"SELECT * FROM read_parquet('{r_file}') LIMIT 0").description]
    if "asset" in cols:
        assets = con.execute(f"""
            SELECT asset, duration, COUNT(*) as cnt,
                   SUM(CASE WHEN outcome = 'Up' THEN 1 ELSE 0 END) as up_cnt
            FROM read_parquet('{r_file}')
            WHERE resolved = true
            GROUP BY asset, duration
        """).fetchdf()
        print(f"\n  By market type:")
        for _, r in assets.iterrows():
            up_pct = r['up_cnt'] / r['cnt'] * 100 if r['cnt'] > 0 else 0
            print(f"    {r['asset']}-{r['duration']}: {r['cnt']:,} markets (Up: {up_pct:.1f}%)")

    con.close()


def report_coverage(cfg):
    """Cross-reference orderbook, trades, and resolutions with detailed breakdown."""
    cid_file = condition_ids_path(cfg)
    if not cid_file.exists():
        return

    with open(cid_file) as f:
        cids = json.load(f)

    cid_to_slug = {v: k for k, v in cids.items()}

    ob_dir = orderbook_dir(cfg)
    ob_files = sorted(ob_dir.glob("orderbook_*.parquet"))
    ob_files = [f for f in ob_files if "all" not in f.name]

    con = duckdb.connect()

    all_cid_set = set(cids.values())

    # CIDs in orderbook (with row counts and update types)
    ob_cids = set()
    ob_rows = {}  # cid -> row count
    ob_snapshots = set()  # cids with book_snapshot data
    if ob_files:
        file_list = ", ".join(f"'{f}'" for f in ob_files)
        for row in con.execute(f"""
            SELECT market_id, COUNT(*) as cnt,
                   SUM(CASE WHEN data LIKE '%book_snapshot%' THEN 1 ELSE 0 END) as snap_cnt
            FROM read_parquet([{file_list}])
            GROUP BY market_id
        """).fetchall():
            ob_cids.add(row[0])
            ob_rows[row[0]] = row[1]
            if row[2] > 0:
                ob_snapshots.add(row[0])

    # CIDs in trades (with counts)
    trade_cids = set()
    trade_counts = {}
    t_file = trades_dir(cfg) / "trades.parquet"
    if t_file.exists():
        for row in con.execute(f"""
            SELECT condition_id, COUNT(*) as cnt
            FROM read_parquet('{t_file}')
            GROUP BY condition_id
        """).fetchall():
            trade_cids.add(row[0])
            trade_counts[row[0]] = row[1]

    # CIDs in resolutions
    res_cids = set()
    r_file = data_dir(cfg) / "resolutions.parquet"
    if r_file.exists():
        for row in con.execute(f"""
            SELECT DISTINCT condition_id FROM read_parquet('{r_file}')
        """).fetchall():
            res_cids.add(row[0])

    con.close()

    has_ob = all_cid_set & ob_cids
    has_snapshots = all_cid_set & ob_snapshots
    has_trades = all_cid_set & trade_cids
    has_res = all_cid_set & res_cids
    has_all = has_ob & has_trades & has_res
    no_ob = all_cid_set - ob_cids
    no_trades = all_cid_set - trade_cids

    # Summary
    print(f"  Configured markets:          {len(all_cid_set)}")
    print(f"  With orderbook data:         {len(has_ob):>4} ({len(has_ob)/len(all_cid_set)*100:.0f}%)")
    print(f"    - with book_snapshots:     {len(has_snapshots):>4} (full depth orderbook)")
    print(f"    - price_changes only:      {len(has_ob - has_snapshots):>4} (order activity, no full book)")
    print(f"  With trade data:             {len(has_trades):>4} ({len(has_trades)/len(all_cid_set)*100:.0f}%)")
    print(f"  With resolution data:        {len(has_res):>4} ({len(has_res)/len(all_cid_set)*100:.0f}%)")
    print(f"  With ALL three:              {len(has_all):>4} ({len(has_all)/len(all_cid_set)*100:.0f}%)")
    print(f"  Missing orderbook:           {len(no_ob):>4}")
    print(f"  Missing trades:              {len(no_trades):>4}")

    # Breakdown by market type
    type_stats = defaultdict(lambda: {"total": 0, "ob": 0, "snap": 0, "trades": 0, "res": 0, "all": 0})
    for slug, cid in cids.items():
        # Parse type from slug: btc-updown-5m-{ts} -> btc-5m
        parts = slug.split("-")
        if len(parts) >= 4:
            mtype = f"{parts[0]}-{parts[2]}"
        else:
            mtype = "unknown"
        type_stats[mtype]["total"] += 1
        if cid in has_ob:
            type_stats[mtype]["ob"] += 1
        if cid in has_snapshots:
            type_stats[mtype]["snap"] += 1
        if cid in has_trades:
            type_stats[mtype]["trades"] += 1
        if cid in has_res:
            type_stats[mtype]["res"] += 1
        if cid in has_all:
            type_stats[mtype]["all"] += 1

    print(f"\n  By market type:")
    print(f"  {'Type':>10}  {'Total':>5}  {'Orderbook':>10}  {'Snapshots':>10}  {'Trades':>8}  {'Resolved':>8}  {'All 3':>7}")
    print(f"  {'-'*10}  {'-'*5}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*7}")
    for mtype in sorted(type_stats):
        s = type_stats[mtype]
        print(f"  {mtype:>10}  {s['total']:5d}  {s['ob']:4d} ({s['ob']/s['total']*100:4.0f}%)  "
              f"{s['snap']:4d} ({s['snap']/s['total']*100:4.0f}%)  "
              f"{s['trades']:4d} ({s['trades']/s['total']*100:3.0f}%)  "
              f"{s['res']:4d} ({s['res']/s['total']*100:3.0f}%)  "
              f"{s['all']:4d} ({s['all']/s['total']*100:3.0f}%)")

    # Orderbook coverage by market start hour (UTC)
    if has_ob or no_ob:
        print(f"\n  Orderbook coverage by market start hour (UTC):")
        hour_stats = defaultdict(lambda: {"total": 0, "with_ob": 0})
        for slug, cid in cids.items():
            try:
                ts = int(slug.rsplit("-", 1)[1])
                h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
            except (ValueError, IndexError):
                continue
            hour_stats[h]["total"] += 1
            if cid in has_ob:
                hour_stats[h]["with_ob"] += 1

        for h in range(24):
            s = hour_stats[h]
            if s["total"] == 0:
                continue
            pct = s["with_ob"] / s["total"] * 100
            filled = int(pct / 5)  # each # = 5%
            bar = "#" * filled + "." * (20 - filled)
            print(f"    {h:02d}:00  {s['with_ob']:3d}/{s['total']:3d}  ({pct:5.1f}%)  {bar}")

    # Orderbook data quality for markets that DO have data
    if has_ob:
        row_counts = [ob_rows[cid] for cid in has_ob]
        row_counts.sort()
        median_rows = row_counts[len(row_counts) // 2]
        print(f"\n  Orderbook row distribution (for {len(has_ob)} markets with data):")
        print(f"    Min: {row_counts[0]:,}, Median: {median_rows:,}, Max: {row_counts[-1]:,}")

        # Bucket by row count
        buckets = [(1, 10), (11, 100), (101, 1000), (1001, 10000), (10001, 1000000)]
        for lo, hi in buckets:
            n = sum(1 for r in row_counts if lo <= r <= hi)
            if n > 0:
                print(f"    {lo:>6}-{hi:>7} rows: {n:4d} markets")

    # Markets missing both orderbook and trade data
    missing_both = no_ob & no_trades
    if missing_both:
        print(f"\n  WARNING: {len(missing_both)} markets have NEITHER orderbook nor trade data")


def main():
    parser = argparse.ArgumentParser(description="Report on downloaded data")
    parser.add_argument("--summary", action="store_true", help="Short summary only")
    args = parser.parse_args()

    cfg = load_config()

    print("=" * 60)
    print("PMXT Data Report")
    print("=" * 60)

    print(f"\nConfig: {cfg['start_date']} to {cfg['end_date']}")
    for m in cfg["markets"]:
        print(f"  {m['asset']}-updown-{m['duration']}")

    print(f"\n--- Orderbook Data ---")
    report_orderbook(cfg)

    if not args.summary:
        print(f"\n--- Trade Data ---")
        report_trades(cfg)

        print(f"\n--- Resolutions ---")
        report_resolutions(cfg)

    print(f"\n--- Coverage ---")
    report_coverage(cfg)


if __name__ == "__main__":
    main()
