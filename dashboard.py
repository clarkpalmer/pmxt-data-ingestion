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
    discover_condition_ids, get_archive_file_list, archive_endpoints,
    duration_seconds, market_slug,
    download_markets_snapshot, find_markets_snapshot, snapshot_status,
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


def _partition_readable_parquets(con, files):
    """Split parquet files into (readable, corrupt) by probing each footer.

    A truncated download (no magic bytes at end of file) would otherwise
    crash every query that touches the file list.
    """
    good, corrupt = [], []
    for f in files:
        try:
            con.execute(f"SELECT 1 FROM read_parquet('{f}') LIMIT 1").fetchall()
            good.append(f)
        except Exception:
            corrupt.append(f)
    return good, corrupt


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


# The dashboard form edits only dates + market selectors. Everything else in
# config.yaml (archive_version, data_dir, download settings, markets_snapshot,
# any user-added keys) is preserved verbatim — the form must never clobber
# configuration it doesn't display.
@app.route("/api/config", methods=["POST"])
def save_config():
    try:
        posted = request.json or {}
        try:
            with open(CONFIG_FILE) as f:
                cfg = yaml.safe_load(f) or {}
        except FileNotFoundError:
            cfg = {}
        for key in ("start_date", "end_date"):
            if posted.get(key):
                cfg[key] = posted[key]
        for key in ("markets", "slugs", "condition_ids", "event_ids", "tags"):
            if posted.get(key):
                cfg[key] = posted[key]
            else:
                # Selector cleared in the form -> remove it
                cfg.pop(key, None)
        tmp = CONFIG_FILE.parent / (CONFIG_FILE.name + ".tmp")
        with open(tmp, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
        tmp.replace(CONFIG_FILE)
        # Date range may have changed — the archive scan is range-dependent
        _archive_cache["urls"] = None
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
    target_hours = hour_range(cfg)
    # The checkpoint accumulates hours across every range ever downloaded;
    # the headline stat must count only hours inside the CURRENT range.
    target_set = set(target_hours)
    downloaded = set()
    for fname in cp.get("downloaded_hours", []):
        m = re.search(r"(\d{4}-\d{2}-\d{2})T(\d{2})", str(fname))
        if m and (m.group(1), m.group(2)) in target_set:
            downloaded.add((m.group(1), m.group(2)))

    ob_dir = orderbook_dir(cfg)
    ob_files = sorted(ob_dir.glob("orderbook_*.parquet"))
    ob_files = [f for f in ob_files if "all" not in f.name]

    total_rows = 0
    total_size = 0
    files_info = []
    con = duckdb.connect()
    ob_files, corrupt_files = _partition_readable_parquets(con, ob_files)
    for f in ob_files:
        n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{f}')").fetchone()[0]
        mb = f.stat().st_size / 1e6
        total_rows += n
        total_size += mb
        files_info.append({"name": f.name, "rows": n, "size_mb": round(mb, 1)})
    for f in corrupt_files:
        files_info.append({
            "name": f.name, "rows": None,
            "size_mb": round(f.stat().st_size / 1e6, 1), "corrupt": True,
        })
    con.close()

    # Same corrupt-file tolerance as the orderbook files: a truncated
    # trades/resolutions parquet must flag, not 500 the whole page.
    t_file = trades_dir(cfg) / "trades.parquet"
    trades_info = None
    if t_file.exists():
        try:
            con = duckdb.connect()
            tn = con.execute(f"SELECT COUNT(*) FROM read_parquet('{t_file}')").fetchone()[0]
            con.close()
            trades_info = {"rows": tn, "size_mb": round(t_file.stat().st_size / 1e6, 1)}
        except Exception:
            trades_info = {"rows": None, "corrupt": True,
                           "size_mb": round(t_file.stat().st_size / 1e6, 1)}

    r_file = data_dir(cfg) / "resolutions.parquet"
    res_info = None
    if r_file.exists():
        try:
            con = duckdb.connect()
            rn = con.execute(f"SELECT COUNT(*) FROM read_parquet('{r_file}')").fetchone()[0]
            con.close()
            res_info = {"rows": rn}
        except Exception:
            res_info = {"rows": None, "corrupt": True}

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


# --- Markets Snapshot API ---

_snap_job = {
    "running": False, "bytes_done": 0, "bytes_total": None,
    "error": None, "done": False,
}


@app.route("/api/snapshot")
def get_snapshot():
    info = snapshot_status() or {"present": False}
    if "path" in info:
        info["present"] = True
    info.update({
        "downloading": _snap_job["running"],
        "bytes_done": _snap_job["bytes_done"],
        "bytes_total": _snap_job["bytes_total"],
        "download_error": _snap_job["error"],
        "download_done": _snap_job["done"],
    })
    return jsonify(info)


def _run_snapshot_download():
    def progress(done, total):
        _snap_job["bytes_done"] = done
        _snap_job["bytes_total"] = total

    try:
        download_markets_snapshot(progress=progress)
        _snap_job["done"] = True
    except Exception as e:
        _snap_job["error"] = str(e)
    finally:
        _snap_job["running"] = False


@app.route("/api/snapshot/download", methods=["POST"])
def start_snapshot_download():
    with _job_lock:
        if _snap_job["running"]:
            return jsonify({"error": "Snapshot download already in progress"}), 409
        _snap_job.update(running=True, bytes_done=0, bytes_total=None,
                         error=None, done=False)
    threading.Thread(target=_run_snapshot_download, daemon=True).start()
    return jsonify({"ok": True})


# --- Market Search API ---

def _resolved_outcome(status, result_id, out0, out1):
    if status == "resolved" and result_id in ("0", "1"):
        return out0 if result_id == "0" else out1
    return None


@app.route("/api/search")
def search_markets():
    q = request.args.get("q", "").strip()
    if len(q) < 2:
        return jsonify([])

    # Search local markets snapshot (instant); falls back to Gamma API below
    snapshot = find_markets_snapshot()
    if snapshot:
        try:
            con = duckdb.connect()
            terms = [t.replace("'", "''") for t in q.lower().split()]
            where = " AND ".join(
                f"(LOWER(slug) LIKE '%{t}%' OR LOWER(question) LIKE '%{t}%'"
                f" OR LOWER(event_title) LIKE '%{t}%' OR market_id LIKE '%{t}%')"
                for t in terms
            )
            rows = con.execute(f"""
                SELECT slug, question, market_id, event_id, event_title,
                       tags, status, result_id, outcome_0, outcome_1
                FROM read_parquet('{snapshot}')
                WHERE {where}
                LIMIT 50
            """).fetchall()
            con.close()

            results = []
            for slug, question, cid, eid, etitle, tags, status, rid, out0, out1 in rows:
                results.append({
                    "slug": slug or "",
                    "question": question or etitle or "",
                    "condition_id": cid or "",
                    "event_id": str(eid or ""),
                    "tags": tags or [],
                    "status": status or "",
                    "resolved_outcome": _resolved_outcome(status, rid, out0, out1),
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


@app.route("/api/market/<path:key>")
def market_lookup(key):
    """Full metadata for one market, by exact slug or condition ID (snapshot)."""
    snapshot = find_markets_snapshot()
    if snapshot is None:
        return jsonify({"error": "No markets snapshot — download one first"}), 404

    k = key.strip().replace("'", "''")
    con = duckdb.connect()
    try:
        row = con.execute(f"""
            SELECT slug, market_id, event_id, event_slug, event_title, question,
                   category, outcome_0, outcome_1, asset_id_0, asset_id_1,
                   status, result_id, settled_at_us, start_date_us, end_date_us,
                   tags, trades_from, trades_to,
                   book_snapshot_full_from, book_snapshot_full_to
            FROM read_parquet('{snapshot}')
            WHERE slug = '{k}' OR market_id = '{k}'
            LIMIT 1
        """).fetchone()
    finally:
        con.close()

    if row is None:
        return jsonify({"error": f"No market matching {key!r} in snapshot"}), 404

    (slug, cid, eid, eslug, etitle, question, category, out0, out1,
     aid0, aid1, status, rid, settled_us, start_us, end_us,
     tags, t_from, t_to, b_from, b_to) = row
    return jsonify({
        "slug": slug, "condition_id": cid,
        "event_id": str(eid or ""), "event_slug": eslug, "event_title": etitle,
        "question": question, "category": category,
        "outcomes": [out0, out1], "asset_ids": [aid0, aid1],
        "status": status,
        "resolved_outcome": _resolved_outcome(status, rid, out0, out1),
        "settled_at_us": settled_us,
        "start_date_us": start_us, "end_date_us": end_us,
        "tags": tags or [],
        "coverage": {
            "trades": [t_from, t_to],
            "book_snapshot_full": [b_from, b_to],
        },
    })


# --- Archive Freshness API ---

_archive_cache = {
    "fetched_at": 0.0, "urls": None, "truncated": False, "oldest_scanned": None,
    "scan_failed": False, "head_checked": {},
}
_ARCHIVE_CACHE_TTL = 300
_ARCHIVE_MAX_PAGES = 60
_HEAD_CHECK_CAP = 30


def _verify_missing_hours(cfg, urls, keys):
    """The archive listing returns inconsistent pages between scans, so a key
    absent from the listing is not necessarily absent from the archive.
    HEAD-check the predicted file URL for each candidate.

    Returns (exists, verified): keys confirmed to exist, and the set of keys
    actually checked this TTL window. Candidates we never got to (beyond the
    per-call cap) must NOT be labeled missing — the caller renders them as
    unverified. Already-checked keys don't consume cap budget, so successive
    polls advance through the candidate list.
    """
    if not urls:
        return set(), set()
    sample = next(iter(urls))
    if not re.search(r"\d{4}-\d{2}-\d{2}T\d{2}", sample):
        return set(), set()
    ep = archive_endpoints(cfg)
    session = requests.Session()
    checked = _archive_cache["head_checked"]
    exists = {k for k in keys if checked.get(k)}
    verified = {k for k in keys if k in checked}
    unchecked = [k for k in keys if k not in checked]
    for key in unchecked[:_HEAD_CHECK_CAP]:
        fname = re.sub(r"\d{4}-\d{2}-\d{2}T\d{2}", f"{key[0]}T{key[1]}", sample)
        try:
            resp = session.head(f"{ep['download']}/{fname}", timeout=10, allow_redirects=True)
            # Only a definitive 404/410 proves absence; transient errors stay unverified
            if resp.status_code == 200:
                checked[key] = True
            elif resp.status_code in (404, 410):
                checked[key] = False
            else:
                continue
        except Exception:
            continue
        verified.add(key)
        if checked[key]:
            exists.add(key)
    return exists, verified


def _scan_archive(cfg, oldest_needed):
    """Page through the archive listing (newest-first) until we have covered
    oldest_needed, the listing is exhausted, or the page cap is hit.

    Returns (urls, truncated, oldest_scanned, failed). failed=True means the
    scan produced nothing because of network errors — which must render as
    "couldn't scan", never as "everything is missing from the archive".
    """
    ep = archive_endpoints(cfg)
    session = requests.Session()
    urls = {}
    oldest_scanned = None
    consecutive_empty = 0
    truncated = True
    errors = 0
    for page in range(1, _ARCHIVE_MAX_PAGES + 1):
        try:
            u = ep["archive"] if page == 1 else f"{ep['archive']}?page={page}"
            resp = session.get(u, timeout=30)
            found = set(re.findall(ep["url_pattern"], resp.text))
        except Exception:
            errors += 1
            found = set()
        if not found:
            consecutive_empty += 1
            if consecutive_empty >= 3:
                truncated = False  # listing exhausted
                break
            continue
        consecutive_empty = 0
        for url in found:
            fname = url.rsplit("/", 1)[1]
            urls[fname] = url
            m = re.search(r"(\d{4}-\d{2}-\d{2})T(\d{2})", fname)
            if m:
                key = (m.group(1), m.group(2))
                if oldest_scanned is None or key < oldest_scanned:
                    oldest_scanned = key
        if oldest_needed and oldest_scanned and oldest_scanned <= oldest_needed:
            truncated = False  # covered everything we need
            break
    failed = not urls and errors > 0
    return urls, truncated, oldest_scanned, failed


@app.route("/api/archive")
def archive_status():
    try:
        cfg = load_config()
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    target = hour_range(cfg)
    oldest_needed = min(target) if target else None

    now = time.time()
    force = request.args.get("force") in ("1", "true")
    if (force or _archive_cache["urls"] is None
            or now - _archive_cache["fetched_at"] > _ARCHIVE_CACHE_TTL):
        urls, truncated, oldest_scanned, failed = _scan_archive(cfg, oldest_needed)
        _archive_cache.update(
            fetched_at=now, urls=urls, truncated=truncated, oldest_scanned=oldest_scanned,
            scan_failed=failed, head_checked={},
        )
    urls = _archive_cache["urls"]
    truncated = _archive_cache["truncated"]
    oldest_scanned = _archive_cache["oldest_scanned"]
    scan_failed = _archive_cache.get("scan_failed", False)

    available = set()
    latest = None
    for fname in urls:
        m = re.search(r"(\d{4}-\d{2}-\d{2})T(\d{2})", fname)
        if m:
            key = (m.group(1), m.group(2))
            available.add(key)
            if latest is None or key > latest:
                latest = key

    cp = load_checkpoint(cfg)
    downloaded = set()
    for fname in cp.get("downloaded_hours", []):
        m = re.search(r"(\d{4}-\d{2}-\d{2})T(\d{2})", str(fname))
        if m:
            downloaded.add((m.group(1), m.group(2)))

    # Per-hour state within the configured range:
    #   downloaded > available > pending (newer than latest archive file)
    #   > unknown (older than our truncated scan, scan failure, or not yet
    #     HEAD-verified) > missing (HEAD-verified archive gap)
    # Listing pages are inconsistent between scans, so a key may only render
    # as "missing" after a definitive HEAD 404 on its predicted file URL.
    candidates = set()
    if not scan_failed:
        for key in target:
            if (key not in downloaded and key not in available
                    and not (latest and key > latest)
                    and not (truncated and oldest_scanned and key < oldest_scanned)):
                candidates.add(key)
    exists, verified = _verify_missing_hours(cfg, urls, sorted(candidates))
    available |= exists

    hours = []
    for key in target:
        d, h = key
        if key in downloaded:
            state = "downloaded"
        elif key in available:
            state = "available"
        elif latest and key > latest:
            state = "pending"
        elif key in candidates and key in verified:
            state = "missing"
        else:
            state = "unknown"
        hours.append({"date": d, "hour": h, "state": state})

    latest_str = None
    latest_end_ts = None
    if latest:
        latest_dt = datetime.strptime(f"{latest[0]}T{latest[1]}", "%Y-%m-%dT%H").replace(
            tzinfo=timezone.utc
        )
        latest_str = f"{latest[0]}T{latest[1]}"
        # The file covers a full hour, so archive data extends to file start + 1h
        latest_end_ts = int(latest_dt.timestamp()) + 3600

    return jsonify({
        "latest_file_hour": latest_str,
        "latest_end_ts": latest_end_ts,
        "age_seconds": (int(now) - latest_end_ts) if latest_end_ts else None,
        "files_seen": len(urls),
        "scan_truncated": truncated,
        "scan_failed": scan_failed,
        "hours": hours,
        "missing": [f"{d}T{h}" for d, h, in
                    [(x["date"], x["hour"]) for x in hours if x["state"] == "missing"]],
        "cached_age_seconds": int(now - _archive_cache["fetched_at"]),
    })


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
        prev_cids = set(cp.get("discovered_cids", {}).values())
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

        # Hours already checkpointed were filtered with the OLD condition-ID
        # set — new markets get no data for those hours unless re-downloaded.
        if prev_cids and (cid_set - prev_cids) and cp.get("downloaded_hours"):
            _log(f"WARNING: {len(cid_set - prev_cids)} new condition IDs since the "
                 f"last download — already-downloaded hours were filtered without "
                 f"them and will NOT be re-fetched. Delete data/checkpoint.json "
                 f"(downloaded_hours) to re-download those hours for the new markets.")

        # Phase 2: Get archive file list
        _job["phase"] = "listing"
        _log("Fetching archive file list...")
        all_urls = get_archive_file_list(cfg)
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

        from download import (download_file, filter_parquet,
                              hourly_chunk_dir, merge_hourly_to_daily)
        ob_dir = orderbook_dir(cfg)
        hourly_dir = hourly_chunk_dir(cfg)

        if not to_process:
            _log("Nothing to download!")
            # Sweep chunks stranded by an interrupted earlier run — they're
            # checkpointed as downloaded and would otherwise never be merged.
            for date_str in date_range(cfg):
                merge_hourly_to_daily(hourly_dir, date_str, ob_dir)
            _job["phase"] = "done"
            _job["done"] = True
            return

        # Phase 3: Download and filter
        _job["phase"] = "downloading"
        _job["total"] = len(to_process)
        temp_dir = Path(cfg.get("download", {}).get("temp_dir", "/tmp/pmxt_ingestion"))
        temp_dir.mkdir(parents=True, exist_ok=True)
        connections = cfg.get("download", {}).get("connections", 4)

        total_rows = 0
        for i, (fname, url, date_str, hour_str) in enumerate(to_process):
            _job["progress"] = i + 1
            _log(f"[{i+1}/{len(to_process)}] {fname}")

            raw_file = temp_dir / fname
            try:
                ok = download_file(url, raw_file, connections)
                if not ok:
                    _log("  DOWNLOAD FAILED")
                    continue

                out_file = hourly_dir / f"chunk_{date_str}_T{hour_str}.parquet"
                n = filter_parquet(raw_file, cid_set, out_file)
                total_rows += n
                _log(f"  -> {n:,} rows")
            except Exception as e:
                # One bad archive file must not abort the whole job
                _log(f"  ERROR: {e}")
                continue
            finally:
                raw_file.unlink(missing_ok=True)

            cp.setdefault("downloaded_hours", []).append(fname)
            save_checkpoint(cfg, cp)

        # Phase 4: Merge (full configured range, so stranded chunks from
        # earlier interrupted runs are swept in too)
        _job["phase"] = "merging"
        _log("Merging into daily files...")
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

    ob_files, corrupt_files = _partition_readable_parquets(con, ob_files)

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
        try:
            for row in con.execute(f"""
                SELECT DISTINCT condition_id FROM read_parquet('{t_file}')
            """).fetchall():
                trade_cids.add(row[0])
        except Exception:
            corrupt_files.append(t_file)

    res_cids = set()
    r_file = data_dir(cfg) / "resolutions.parquet"
    if r_file.exists():
        try:
            for row in con.execute(f"""
                SELECT DISTINCT condition_id FROM read_parquet('{r_file}')
            """).fetchall():
                res_cids.add(row[0])
        except Exception:
            corrupt_files.append(r_file)

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

    # Hourly coverage (updown slugs only — their last token is a timestamp;
    # arbitrary slugs ending in a number would mis-bucket)
    hourly = {}
    for slug, cid in cids.items():
        if "updown" not in slug:
            continue
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
        "corrupt_files": [f.name for f in corrupt_files],
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
