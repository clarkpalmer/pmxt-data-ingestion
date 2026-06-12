#!/usr/bin/env python3
"""Manage the local Polymarket markets snapshot.

Telonex serves the full Polymarket markets dataset (1.28M+ markets) as a
single parquet at a free public endpoint — no API key. It carries slugs,
condition IDs, questions, outcomes, resolution status/result, tags, and
per-channel data-coverage dates.

A local copy powers:
- instant full-text market search in the dashboard
- offline slug -> condition-ID resolution during downloads (no Gamma rate limit)
- market metadata lookups (resolutions, titles, tags, coverage)

Usage:
    python snapshot.py              # show status, offer to download/refresh
    python snapshot.py --yes        # download/refresh without asking
    python snapshot.py --status     # status only, never download
    python snapshot.py --lookup X   # look up a market by slug or condition ID
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from common import (
    TELONEX_MARKETS_URL,
    download_markets_snapshot, find_markets_snapshot,
    snapshot_dest, snapshot_status,
)


def fmt_age(seconds):
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h {seconds % 3600 // 60}m"
    return f"{seconds // 86400}d {seconds % 86400 // 3600}h"


def print_status(info):
    if info is None:
        print("No markets snapshot found.")
        print(f"  Would download to: {snapshot_dest()}")
        return
    print(f"Markets snapshot: {info['path']}")
    print(f"  size: {info['size_mb']:.0f} MB")
    print(f"  age:  {fmt_age(info['age_seconds'])} "
          f"(downloaded datasets refresh continuously upstream)")
    if info.get("rows"):
        print(f"  rows: {info['rows']:,} markets")


def run_download():
    print(f"Downloading markets snapshot from {TELONEX_MARKETS_URL}")
    dest = snapshot_dest()
    t0 = time.time()
    last_print = [0.0]

    def progress(done, total):
        now = time.time()
        if now - last_print[0] < 2 and (total is None or done < total):
            return
        last_print[0] = now
        elapsed = now - t0
        rate = done / elapsed / 1e6 if elapsed > 0 else 0
        if total:
            eta = (total - done) / (done / elapsed) if done else 0
            print(f"  {done / 1e6:.0f}/{total / 1e6:.0f} MB "
                  f"({elapsed:.0f}s, {rate:.0f} MB/s, ETA {eta:.0f}s)", flush=True)
        else:
            print(f"  {done / 1e6:.0f} MB ({elapsed:.0f}s, {rate:.0f} MB/s)", flush=True)

    path = download_markets_snapshot(dest, progress=progress)
    print(f"Done in {time.time() - t0:.0f}s -> {path}")
    print_status(snapshot_status())


def run_lookup(query):
    snapshot = find_markets_snapshot()
    if snapshot is None:
        print("No markets snapshot found — run `python snapshot.py` to download one.")
        sys.exit(1)

    import duckdb
    con = duckdb.connect()
    q = query.replace("'", "''")
    base = f"""
        SELECT slug, market_id, question, event_title, status, result_id,
               outcome_0, outcome_1, settled_at_us, tags,
               trades_from, trades_to, book_snapshot_full_from, book_snapshot_full_to
        FROM read_parquet('{snapshot}')
    """
    rows = con.execute(base + f"WHERE slug = '{q}' OR market_id = '{q}' LIMIT 5").fetchall()
    if not rows:
        rows = con.execute(
            base + f"WHERE slug LIKE '%{q}%' OR question ILIKE '%{q}%' LIMIT 10"
        ).fetchall()
    con.close()

    if not rows:
        print(f"No market matching {query!r} in the snapshot.")
        sys.exit(1)

    for (slug, cid, question, event_title, status, result_id,
         out0, out1, settled_us, tags, t_from, t_to, b_from, b_to) in rows:
        print(f"slug:         {slug}")
        print(f"condition_id: {cid}")
        print(f"question:     {question or event_title or ''}")
        line = f"status:       {status}"
        if status == "resolved" and result_id in ("0", "1"):
            winner = out0 if result_id == "0" else out1
            when = ""
            if settled_us:
                when = " at " + datetime.fromtimestamp(
                    settled_us / 1e6, tz=timezone.utc
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
            line += f" -> {winner}{when}"
        print(line)
        print(f"outcomes:     {out0} / {out1}")
        if tags:
            print(f"tags:         {', '.join(tags)}")
        if t_from or b_from:
            print(f"coverage:     trades {t_from or '?'}..{t_to or '?'}, "
                  f"full book {b_from or '?'}..{b_to or '?'}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Manage the local markets snapshot")
    parser.add_argument("--status", action="store_true", help="Show status only")
    parser.add_argument("--yes", "-y", action="store_true",
                        help="Download/refresh without asking")
    parser.add_argument("--lookup", metavar="SLUG_OR_CID",
                        help="Look up a market by slug or condition ID")
    args = parser.parse_args()

    if args.lookup:
        run_lookup(args.lookup)
        return

    info = snapshot_status()
    print_status(info)

    if args.status:
        return

    if args.yes:
        run_download()
        return

    # Ask before pulling ~600+ MB
    if not sys.stdin.isatty():
        print("\nNot a TTY — pass --yes to download non-interactively.")
        return
    verb = "Refresh" if info else "Download"
    answer = input(f"\n{verb} the snapshot now (~600+ MB, free, no API key)? [y/N] ")
    if answer.strip().lower() in ("y", "yes"):
        run_download()
    else:
        print("Skipped.")


if __name__ == "__main__":
    main()
