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

REPORTS_DIR = Path(__file__).parent / "reports"


def _detect_ob_schema(con, filepath):
    """Detect orderbook parquet schema version. Returns (market_col, type_col, is_v2)."""
    cols = [row[0] for row in con.execute(
        f"SELECT name FROM parquet_schema('{filepath}')"
    ).fetchall()]
    if 'market' in cols and 'market_id' not in cols:
        return "CAST(market AS VARCHAR)", "event_type", True
    return "market_id", "update_type", False


class ReportBuilder:
    """Collects report lines for both terminal and markdown output."""

    def __init__(self):
        self._lines = []

    def print(self, text=""):
        """Print to terminal and collect for markdown."""
        print(text)
        self._lines.append(text)

    def write_markdown(self, cfg):
        """Write collected output as a markdown file."""
        REPORTS_DIR.mkdir(exist_ok=True)
        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        filename = REPORTS_DIR / f"report_{timestamp}.md"

        md_lines = []
        md_lines.append(f"# PMXT Data Report")
        md_lines.append("")
        md_lines.append(f"*Generated: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC*")
        md_lines.append("")
        md_lines.append("```")
        md_lines.extend(self._lines)
        md_lines.append("```")

        filename.write_text("\n".join(md_lines) + "\n")
        print(f"\nReport saved to {filename}")


def report_orderbook(cfg, out):
    """Report on orderbook data."""
    ob_dir = orderbook_dir(cfg)
    files = sorted(ob_dir.glob("orderbook_*.parquet"))
    merged = [f for f in files if "all" not in f.name]

    if not merged:
        out.print("  No orderbook files found.")
        return

    out.print(f"  Files: {len(merged)}")

    con = duckdb.connect()
    total_rows = 0
    total_size = 0

    market_col, type_col, _ = _detect_ob_schema(con, merged[0])

    for f in merged:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        mb = f.stat().st_size / 1e6
        total_rows += n
        total_size += mb

        types = con.execute(f"""
            SELECT {type_col} as utype, COUNT(*) as cnt
            FROM read_parquet('{f}')
            GROUP BY {type_col}
        """).fetchall()
        type_str = ", ".join(f"{r[0]}={r[1]:,}" for r in types)
        out.print(f"  {f.name}: {n:,} rows ({mb:.1f} MB) [{type_str}]")

    out.print(f"  Total: {total_rows:,} rows ({total_size:.1f} MB)")

    if merged:
        file_list = ", ".join(f"'{f}'" for f in merged)
        n_markets = con.execute(f"""
            SELECT COUNT(DISTINCT {market_col})
            FROM read_parquet([{file_list}])
        """).fetchone()[0]
        out.print(f"  Unique markets (condition IDs): {n_markets}")

    con.close()


def report_trades(cfg, out):
    """Report on trade data."""
    t_file = trades_dir(cfg) / "trades.parquet"
    if not t_file.exists():
        out.print("  No trades file found. Run: python enrich.py --trades")
        return

    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{t_file}')").fetchone()[0]
    mb = t_file.stat().st_size / 1e6
    out.print(f"  File: trades.parquet ({n:,} rows, {mb:.1f} MB)")

    # Markets with trades
    n_markets = con.execute(f"""
        SELECT COUNT(DISTINCT condition_id) FROM read_parquet('{t_file}')
    """).fetchone()[0]
    out.print(f"  Markets with trades: {n_markets}")

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
        out.print(f"  Time range: {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC")
    if stats[1]:
        out.print(f"  Total volume: ${stats[1]:,.0f}")

    # By side
    sides = con.execute(f"""
        SELECT side, COUNT(*) as cnt, SUM(size) as total_size
        FROM read_parquet('{t_file}')
        GROUP BY side
    """).fetchall()
    for side, cnt, total_size in sides:
        out.print(f"  {side}: {cnt:,} trades, {total_size:,.0f} shares")

    con.close()


def report_resolutions(cfg, out):
    """Report on resolution data."""
    r_file = data_dir(cfg) / "resolutions.parquet"
    if not r_file.exists():
        out.print("  No resolutions file found. Run: python enrich.py --resolutions")
        return

    con = duckdb.connect()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{r_file}')").fetchone()[0]
    out.print(f"  File: resolutions.parquet ({n:,} rows)")

    # Resolution stats
    resolved = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{r_file}') WHERE resolved = true
    """).fetchone()[0]
    out.print(f"  Resolved: {resolved}/{n}")

    # Outcome distribution
    outcomes = con.execute(f"""
        SELECT outcome, COUNT(*) as cnt
        FROM read_parquet('{r_file}')
        WHERE resolved = true
        GROUP BY outcome
    """).fetchall()
    for outcome, cnt in outcomes:
        pct = cnt / resolved * 100 if resolved > 0 else 0
        out.print(f"  {outcome}: {cnt:,} ({pct:.1f}%)")

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
        out.print(f"  Strike prices: {has_strike:,} markets")
        out.print(f"    Range: ${stats[0]:,.2f} - ${stats[1]:,.2f}")
        out.print(f"    Mean:  ${stats[2]:,.2f}")

    # By asset
    cols = [desc[0] for desc in con.execute(f"SELECT * FROM read_parquet('{r_file}') LIMIT 0").description]
    if "asset" in cols:
        assets = con.execute(f"""
            SELECT asset, duration, COUNT(*) as cnt,
                   SUM(CASE WHEN outcome = 'Up' THEN 1 ELSE 0 END) as up_cnt
            FROM read_parquet('{r_file}')
            WHERE resolved = true
            GROUP BY asset, duration
        """).fetchall()
        out.print(f"\n  By market type:")
        for asset, duration, cnt, up_cnt in assets:
            up_pct = up_cnt / cnt * 100 if cnt > 0 else 0
            out.print(f"    {asset}-{duration}: {cnt:,} markets (Up: {up_pct:.1f}%)")

    con.close()


def report_coverage(cfg, out):
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
        market_col, type_col, is_v2 = _detect_ob_schema(con, ob_files[0])
        if is_v2:
            snap_expr = f"SUM(CASE WHEN {type_col} = 'book_snapshot' THEN 1 ELSE 0 END)"
        else:
            snap_expr = "SUM(CASE WHEN data LIKE '%book_snapshot%' THEN 1 ELSE 0 END)"
        file_list = ", ".join(f"'{f}'" for f in ob_files)
        for row in con.execute(f"""
            SELECT {market_col} as mid, COUNT(*) as cnt,
                   {snap_expr} as snap_cnt
            FROM read_parquet([{file_list}])
            GROUP BY {market_col}
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
    out.print(f"  Configured markets:          {len(all_cid_set)}")
    out.print(f"  With orderbook data:         {len(has_ob):>4} ({len(has_ob)/len(all_cid_set)*100:.0f}%)")
    out.print(f"    - with book_snapshots:     {len(has_snapshots):>4} (full depth orderbook)")
    out.print(f"    - price_changes only:      {len(has_ob - has_snapshots):>4} (order activity, no full book)")
    out.print(f"  With trade data:             {len(has_trades):>4} ({len(has_trades)/len(all_cid_set)*100:.0f}%)")
    out.print(f"  With resolution data:        {len(has_res):>4} ({len(has_res)/len(all_cid_set)*100:.0f}%)")
    out.print(f"  With ALL three:              {len(has_all):>4} ({len(has_all)/len(all_cid_set)*100:.0f}%)")
    out.print(f"  Missing orderbook:           {len(no_ob):>4}")
    out.print(f"  Missing trades:              {len(no_trades):>4}")

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

    out.print(f"\n  By market type:")
    out.print(f"  {'Type':>10}  {'Total':>5}  {'Orderbook':>10}  {'Snapshots':>10}  {'Trades':>8}  {'Resolved':>8}  {'All 3':>7}")
    out.print(f"  {'-'*10}  {'-'*5}  {'-'*10}  {'-'*10}  {'-'*8}  {'-'*8}  {'-'*7}")
    for mtype in sorted(type_stats):
        s = type_stats[mtype]
        out.print(f"  {mtype:>10}  {s['total']:5d}  {s['ob']:4d} ({s['ob']/s['total']*100:4.0f}%)  "
              f"{s['snap']:4d} ({s['snap']/s['total']*100:4.0f}%)  "
              f"{s['trades']:4d} ({s['trades']/s['total']*100:3.0f}%)  "
              f"{s['res']:4d} ({s['res']/s['total']*100:3.0f}%)  "
              f"{s['all']:4d} ({s['all']/s['total']*100:3.0f}%)")

    # Orderbook coverage by market start hour (UTC)
    if has_ob or no_ob:
        out.print(f"\n  Orderbook coverage by market start hour (UTC):")
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
            out.print(f"    {h:02d}:00  {s['with_ob']:3d}/{s['total']:3d}  ({pct:5.1f}%)  {bar}")

    # Orderbook data quality for markets that DO have data
    if has_ob:
        row_counts = [ob_rows[cid] for cid in has_ob]
        row_counts.sort()
        median_rows = row_counts[len(row_counts) // 2]
        out.print(f"\n  Orderbook row distribution (for {len(has_ob)} markets with data):")
        out.print(f"    Min: {row_counts[0]:,}, Median: {median_rows:,}, Max: {row_counts[-1]:,}")

        # Bucket by row count
        buckets = [(1, 10), (11, 100), (101, 1000), (1001, 10000), (10001, 1000000)]
        for lo, hi in buckets:
            n = sum(1 for r in row_counts if lo <= r <= hi)
            if n > 0:
                out.print(f"    {lo:>6}-{hi:>7} rows: {n:4d} markets")

    # Markets missing both orderbook and trade data
    missing_both = no_ob & no_trades
    if missing_both:
        out.print(f"\n  WARNING: {len(missing_both)} markets have NEITHER orderbook nor trade data")


def main():
    parser = argparse.ArgumentParser(description="Report on downloaded data")
    parser.add_argument("--summary", action="store_true", help="Short summary only")
    args = parser.parse_args()

    cfg = load_config()
    out = ReportBuilder()

    out.print("=" * 60)
    out.print("PMXT Data Report")
    out.print("=" * 60)

    out.print(f"\nConfig: {cfg['start_date']} to {cfg['end_date']}")
    for m in cfg["markets"]:
        out.print(f"  {m['asset']}-updown-{m['duration']}")

    out.print(f"\n--- Orderbook Data ---")
    report_orderbook(cfg, out)

    if not args.summary:
        out.print(f"\n--- Trade Data ---")
        report_trades(cfg, out)

        out.print(f"\n--- Resolutions ---")
        report_resolutions(cfg, out)

    out.print(f"\n--- Coverage ---")
    report_coverage(cfg, out)

    out.write_markdown(cfg)


if __name__ == "__main__":
    main()
