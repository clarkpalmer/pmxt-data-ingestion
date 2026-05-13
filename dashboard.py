#!/usr/bin/env python3
"""PMXT Data Ingestion Dashboard — web UI for managing downloads."""

import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import requests
import yaml
from flask import Flask, jsonify, render_template, request

from common import (
    CONFIG_FILE, GAMMA_API,
    load_config, load_checkpoint, save_checkpoint,
    data_dir, orderbook_dir, trades_dir, condition_ids_path,
    date_range, hour_range, timestamp_range,
    discover_condition_ids, get_archive_file_list,
    duration_seconds, market_slug,
)

app = Flask(__name__, template_folder="templates")

# Global job state
_job_lock = threading.Lock()
_job = {
    "running": False,
    "phase": "",
    "progress": 0,
    "total": 0,
    "detail": "",
    "log": [],
    "error": None,
    "done": False,
}


def _reset_job():
    _job.update(running=False, phase="", progress=0, total=0, detail="", log=[], error=None, done=False)


def _log(msg):
    _job["log"].append(msg)
    _job["detail"] = msg


# --- Config API ---

@app.route("/")
def index():
    return render_template("dashboard.html")


@app.route("/api/config", methods=["GET"])
def get_config():
    try:
        with open(CONFIG_FILE) as f:
            cfg = yaml.safe_load(f)
        return jsonify(cfg)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/config", methods=["POST"])
def save_config():
    try:
        cfg = request.json
        with open(CONFIG_FILE, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# --- Data Status API ---

@app.route("/api/status")
def get_status():
    try:
        cfg = load_config()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    cp = load_checkpoint(cfg)
    cids = cp.get("discovered_cids", {})
    downloaded = set(cp.get("downloaded_hours", []))
    target_hours = hour_range(cfg)

    ob_dir = orderbook_dir(cfg)
    ob_files = sorted(ob_dir.glob("orderbook_*.parquet"))
    ob_files = [f for f in ob_files if "all" not in f.name]

    total_rows = 0
    total_size = 0
    files_info = []
    con = duckdb.connect()
    for f in ob_files:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        mb = f.stat().st_size / 1e6
        total_rows += n
        total_size += mb
        files_info.append({"name": f.name, "rows": n, "size_mb": round(mb, 1)})
    con.close()

    t_file = trades_dir(cfg) / "trades.parquet"
    trades_info = None
    if t_file.exists():
        con = duckdb.connect()
        tn = con.execute(f"SELECT COUNT(*) FROM read_parquet('{t_file}')").fetchone()[0]
        con.close()
        trades_info = {"rows": tn, "size_mb": round(t_file.stat().st_size / 1e6, 1)}

    r_file = data_dir(cfg) / "resolutions.parquet"
    res_info = None
    if r_file.exists():
        con = duckdb.connect()
        rn = con.execute(f"SELECT COUNT(*) FROM read_parquet('{r_file}')").fetchone()[0]
        con.close()
        res_info = {"rows": rn}

    return jsonify({
        "config": {
            "start_date": cfg["start_date"],
            "end_date": cfg["end_date"],
            "markets": cfg.get("markets", []),
            "slugs": cfg.get("slugs", []),
            "condition_ids": cfg.get("condition_ids", []),
            "event_ids": cfg.get("event_ids", []),
            "tags": cfg.get("tags", []),
        },
        "condition_ids_count": len(cids),
        "hours_downloaded": len(downloaded),
        "hours_total": len(target_hours),
        "orderbook": {
            "files": files_info,
            "total_rows": total_rows,
            "total_size_mb": round(total_size, 1),
        },
        "trades": trades_info,
        "resolutions": res_info,
    })


# --- Market Search API ---

def _find_markets_snapshot():
    candidates = [
        Path.home() / "Downloads" / "polymarket_markets.parquet",
        Path.home() / "dev" / "ml-polybot" / "datasets" / "polymarket_markets.parquet",
        Path.home() / "dev" / "ml-polybot" / "data" / "metadata" / "polymarket_markets.parquet",
        Path(__file__).parent / "polymarket_markets.parquet",
    ]
    for p in candidates:
        if p.exists():
            return p
    return None


MARKETS_SNAPSHOT = _find_markets_snapshot()


@app.route("/api/search")
def search_markets():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    # Search local markets snapshot (714K markets, instant)
    if MARKETS_SNAPSHOT.exists():
        try:
            con = duckdb.connect()
            terms = q.lower().split()
            where = " AND ".join(
                f"(LOWER(slug) LIKE '%{t}%' OR LOWER(question) LIKE '%{t}%' OR LOWER(event_title) LIKE '%{t}%')"
                for t in terms
            )
            rows = con.execute(f"""
                SELECT slug, question, market_id, event_id, event_title,
                       tags
                FROM read_parquet('{MARKETS_SNAPSHOT}')
                WHERE {where}
                LIMIT 50
            """).fetchall()
            con.close()

            results = []
            for slug, question, cid, eid, etitle, tags in rows:
                results.append({
                    "slug": slug or "",
                    "question": question or etitle or "",
                    "condition_id": cid or "",
                    "event_id": str(eid or ""),
                    "tags": tags or [],
                })
            return jsonify(results)
        except Exception:
            pass

    # Fallback: exact slug lookup via Gamma API
    results = []
    try:
        session = requests.Session()
        resp = session.get(GAMMA_API, params={"slug": q}, timeout=10)
        if resp.ok:
            data = resp.json()
            if data:
                for event in (data if isinstance(data, list) else [data]):
                    for m in event.get("markets", []):
                        results.append({
                            "slug": m.get("slug", event.get("slug", "")),
                            "question": m.get("question", event.get("title", "")),
                            "condition_id": m.get("conditionId", ""),
                            "event_id": str(event.get("id", "")),
                            "tags": [],
                        })
    except Exception:
        pass

    return jsonify(results[:50])


# --- Download Job API ---

@app.route("/api/download", methods=["POST"])
def start_download():
    with _job_lock:
        if _job["running"]:
            return jsonify({"error": "Download already in progress"}), 409
        _reset_job()
        _job["running"] = True

    thread = threading.Thread(target=_run_download, daemon=True)
    thread.start()
    return jsonify({"ok": True})


@app.route("/api/download/status")
def download_status():
    return jsonify({
        "running": _job["running"],
        "phase": _job["phase"],
        "progress": _job["progress"],
        "total": _job["total"],
        "detail": _job["detail"],
        "log": _job["log"][-50:],
        "error": _job["error"],
        "done": _job["done"],
    })


def _run_download():
    try:
        cfg = load_config()
        cp = load_checkpoint(cfg)

        # Phase 1: Discover condition IDs
        _job["phase"] = "discovering"
        _log("Discovering condition IDs...")
        cids = discover_condition_ids(cfg, progress=False)
        cp["discovered_cids"] = cids
        save_checkpoint(cfg, cp)
        with open(condition_ids_path(cfg), "w") as f:
            json.dump(cids, f, indent=2)
        _log(f"Found {len(cids)} condition IDs")

        cid_set = set(cids.values())
        if not cid_set:
            _job["error"] = "No condition IDs found"
            return

        # Phase 2: Get archive file list
        _job["phase"] = "listing"
        _log("Fetching archive file list...")
        all_urls = get_archive_file_list()
        target_hours_set = set(hour_range(cfg))
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
            to_process.append((fname, url, m.group(1), m.group(2)))

        _log(f"Archive files to download: {len(to_process)}")

        if not to_process:
            _log("Nothing to download!")
            _job["phase"] = "done"
            _job["done"] = True
            return

        # Phase 3: Download and filter
        _job["phase"] = "downloading"
        _job["total"] = len(to_process)
        temp_dir = Path(cfg.get("download", {}).get("temp_dir", "/tmp/pmxt_ingestion"))
        temp_dir.mkdir(parents=True, exist_ok=True)
        connections = cfg.get("download", {}).get("connections", 4)
        ob_dir = orderbook_dir(cfg)
        hourly_dir = ob_dir / "hourly"
        hourly_dir.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        for i, (fname, url, date_str, hour_str) in enumerate(to_process):
            _job["progress"] = i + 1
            _log(f"[{i+1}/{len(to_process)}] {fname}")

            raw_file = temp_dir / fname
            from download import download_file, filter_parquet
            ok = download_file(url, raw_file, connections)
            if not ok:
                _log(f"  DOWNLOAD FAILED")
                continue

            out_file = hourly_dir / f"chunk_{date_str}_T{hour_str}.parquet"
            n = filter_parquet(raw_file, cid_set, out_file)
            raw_file.unlink(missing_ok=True)
            total_rows += n

            size_mb = raw_file.stat().st_size / 1e6 if raw_file.exists() else 0
            _log(f"  -> {n:,} rows")

            cp.setdefault("downloaded_hours", []).append(fname)
            save_checkpoint(cfg, cp)

        # Phase 4: Merge
        _job["phase"] = "merging"
        _log("Merging into daily files...")
        from download import merge_hourly_to_daily
        for date_str in date_range(cfg):
            merge_hourly_to_daily(hourly_dir, date_str, ob_dir)

        _log(f"Done! Total rows: {total_rows:,}")
        _job["phase"] = "done"
        _job["done"] = True

    except Exception as e:
        _job["error"] = str(e)
        _log(f"ERROR: {e}")
    finally:
        _job["running"] = False


# --- Coverage Report API ---

@app.route("/api/report")
def get_report():
    try:
        cfg = load_config()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    cp = load_checkpoint(cfg)
    cids = cp.get("discovered_cids", {})
    if not cids:
        return jsonify({"error": "No condition IDs discovered yet"})

    ob_dir = orderbook_dir(cfg)
    ob_files = sorted(ob_dir.glob("orderbook_*.parquet"))
    ob_files = [f for f in ob_files if "all" not in f.name]

    con = duckdb.connect()
    all_cid_set = set(cids.values())

    # Detect schema
    market_col = "market_id"
    type_col = "update_type"
    if ob_files:
        cols = [row[0] for row in con.execute(
            f"SELECT name FROM parquet_schema('{ob_files[0]}')"
        ).fetchall()]
        if 'market' in cols and 'market_id' not in cols:
            market_col = "CAST(market AS VARCHAR)"
            type_col = "event_type"

    ob_cids = set()
    if ob_files:
        file_list = ", ".join(f"'{f}'" for f in ob_files)
        for row in con.execute(f"""
            SELECT DISTINCT {market_col} FROM read_parquet([{file_list}])
        """).fetchall():
            ob_cids.add(row[0])

    trade_cids = set()
    t_file = trades_dir(cfg) / "trades.parquet"
    if t_file.exists():
        for row in con.execute(f"""
            SELECT DISTINCT condition_id FROM read_parquet('{t_file}')
        """).fetchall():
            trade_cids.add(row[0])

    res_cids = set()
    r_file = data_dir(cfg) / "resolutions.parquet"
    if r_file.exists():
        for row in con.execute(f"""
            SELECT DISTINCT condition_id FROM read_parquet('{r_file}')
        """).fetchall():
            res_cids.add(row[0])

    con.close()

    has_ob = all_cid_set & ob_cids
    has_trades = all_cid_set & trade_cids
    has_res = all_cid_set & res_cids
    has_all = has_ob & has_trades & has_res

    # Per-market type breakdown
    type_stats = {}
    for slug, cid in cids.items():
        parts = slug.split("-")
        if len(parts) >= 3 and "updown" in slug:
            mtype = f"{parts[0]}-{parts[2]}"
        else:
            mtype = "other"
        if mtype not in type_stats:
            type_stats[mtype] = {"total": 0, "ob": 0, "trades": 0, "res": 0, "all": 0}
        type_stats[mtype]["total"] += 1
        if cid in has_ob:
            type_stats[mtype]["ob"] += 1
        if cid in has_trades:
            type_stats[mtype]["trades"] += 1
        if cid in has_res:
            type_stats[mtype]["res"] += 1
        if cid in has_all:
            type_stats[mtype]["all"] += 1

    # Hourly coverage
    hourly = {}
    for slug, cid in cids.items():
        try:
            ts = int(slug.rsplit("-", 1)[1])
            hour = datetime.fromtimestamp(ts, tz=timezone.utc).hour
            if hour not in hourly:
                hourly[hour] = {"total": 0, "with_ob": 0}
            hourly[hour]["total"] += 1
            if cid in has_ob:
                hourly[hour]["with_ob"] += 1
        except (ValueError, IndexError):
            pass

    return jsonify({
        "total_markets": len(all_cid_set),
        "with_orderbook": len(has_ob),
        "with_trades": len(has_trades),
        "with_resolutions": len(has_res),
        "with_all": len(has_all),
        "by_type": type_stats,
        "by_hour": {str(h): hourly[h] for h in sorted(hourly)},
    })


def main():
    import argparse
    parser = argparse.ArgumentParser(description="PMXT Dashboard")
    parser.add_argument("--port", type=int, default=5555)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    print(f"Starting PMXT Dashboard at http://{args.host}:{args.port}")
    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()
