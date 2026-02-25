#!/usr/bin/env python3
"""
Build a daily snapshot of Uruguay Open Data catalog health (CKAN-based).

Outputs:
- data/latest.json
- data/history/YYYY-MM-DD.json

Config via env vars:
- CKAN_BASE_URL (default: https://catalogodatos.gub.uy)
- CKAN_API_PATH (default: /api/3/action)  # Action API base path
- MAX_DATASETS (default: 0 -> no limit)
- REQUEST_TIMEOUT_SECONDS (default: 12)
- REQUEST_RETRIES (default: 2)
- REQUEST_DELAY_SECONDS (default: 0.15)
- CSV_SAMPLE_LINES (default: 50)
- FRESH_GREEN_DAYS (default: 90)
- FRESH_YELLOW_DAYS (default: 365)

Notes:
- Read-only API; no key required.
- We intentionally keep checks lightweight for MVP (availability + basic parsing for CSV/JSON).
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from dateutil import parser as date_parser


# ----------------------------
# Config
# ----------------------------
CKAN_BASE_URL = os.getenv("CKAN_BASE_URL", "https://catalogodatos.gub.uy").rstrip("/")
CKAN_API_PATH = os.getenv("CKAN_API_PATH", "/api/3/action").rstrip("/")

MAX_DATASETS = int(os.getenv("MAX_DATASETS", "0"))  # 0 = no limit
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
REQUEST_RETRIES = int(os.getenv("REQUEST_RETRIES", "2"))
REQUEST_DELAY_SECONDS = float(os.getenv("REQUEST_DELAY_SECONDS", "0.15"))

CSV_SAMPLE_LINES = int(os.getenv("CSV_SAMPLE_LINES", "50"))

FRESH_GREEN_DAYS = int(os.getenv("FRESH_GREEN_DAYS", "90"))
FRESH_YELLOW_DAYS = int(os.getenv("FRESH_YELLOW_DAYS", "365"))

OUT_DIR_DATA = os.path.join(os.getcwd(), "data")
OUT_DIR_HISTORY = os.path.join(OUT_DIR_DATA, "history")


# ----------------------------
# Helpers
# ----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def ensure_dirs() -> None:
    os.makedirs(OUT_DIR_DATA, exist_ok=True)
    os.makedirs(OUT_DIR_HISTORY, exist_ok=True)


def safe_parse_datetime(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = date_parser.isoparse(value)
        # Normalize naive datetimes as UTC for consistency
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def days_since(dt: Optional[datetime], now: datetime) -> Optional[int]:
    if dt is None:
        return None
    delta = now - dt
    return max(0, int(delta.total_seconds() // 86400))


def freshness_bucket(days: Optional[int]) -> str:
    # If we don't know, treat as "unknown" (render separately)
    if days is None:
        return "unknown"
    if days < FRESH_GREEN_DAYS:
        return "green"
    if days <= FRESH_YELLOW_DAYS:
        return "yellow"
    return "red"


def action_url(action: str) -> str:
    return f"{CKAN_BASE_URL}{CKAN_API_PATH}/{action}"


def http_get_json(session: requests.Session, url: str, params: Optional[dict] = None) -> dict:
    last_err: Optional[Exception] = None
    for attempt in range(REQUEST_RETRIES + 1):
        try:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            resp.raise_for_status()
            data = resp.json()
            if not data.get("success", False):
                raise RuntimeError(f"CKAN API success=false for {url} params={params}: {data}")
            return data
        except Exception as e:
            last_err = e
            if attempt < REQUEST_RETRIES:
                time.sleep(0.6 * (attempt + 1))
                continue
            raise
    raise last_err  # type: ignore[misc]


@dataclass
class ResourceCheckResult:
    ok: bool
    http_status: Optional[int]
    error: Optional[str]
    bytes_read: int
    parse_ok: Optional[bool]  # None if not attempted
    parse_error: Optional[str]
    checksum: Optional[str]


def headish_download(
    session: requests.Session,
    url: str,
    max_bytes: int = 1024 * 512,  # 512KB
) -> Tuple[int, bytes]:
    """
    Stream-download up to max_bytes. Returns (status_code, content_bytes).
    """
    resp = session.get(url, stream=True, timeout=REQUEST_TIMEOUT_SECONDS, allow_redirects=True)
    status = resp.status_code
    content = b""
    try:
        resp.raise_for_status()
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            remaining = max_bytes - len(content)
            if remaining <= 0:
                break
            content += chunk[:remaining]
            if len(content) >= max_bytes:
                break
    finally:
        resp.close()
    return status, content


def try_parse_csv(sample: bytes) -> Tuple[bool, Optional[str]]:
    """
    Parse a small CSV sample. We don't enforce schema, only readability.
    """
    try:
        text = sample.decode("utf-8", errors="replace")
        # Take first N lines
        lines = text.splitlines()[:CSV_SAMPLE_LINES]
        if not lines:
            return False, "empty sample"
        buf = io.StringIO("\n".join(lines))

        # Sniff delimiter if possible
        sample_for_sniff = "\n".join(lines[:10])
        try:
            dialect = csv.Sniffer().sniff(sample_for_sniff)
        except Exception:
            dialect = csv.excel

        reader = csv.reader(buf, dialect)
        # Read a few rows
        _ = next(reader, None)
        _ = next(reader, None)
        return True, None
    except Exception as e:
        return False, str(e)


def try_parse_json(sample: bytes) -> Tuple[bool, Optional[str]]:
    try:
        text = sample.decode("utf-8", errors="strict")
        json.loads(text)
        return True, None
    except Exception as e:
        return False, str(e)


def checksum_sha256(sample: bytes) -> str:
    return hashlib.sha256(sample).hexdigest()


def check_resource(session: requests.Session, resource: dict) -> ResourceCheckResult:
    url = resource.get("url")
    fmt = (resource.get("format") or "").strip().lower()

    if not url:
        return ResourceCheckResult(
            ok=False,
            http_status=None,
            error="missing url",
            bytes_read=0,
            parse_ok=None,
            parse_error=None,
            checksum=None,
        )

    try:
        status, content = headish_download(session, url)
        bytes_read = len(content)

        if status < 200 or status >= 300:
            return ResourceCheckResult(
                ok=False,
                http_status=status,
                error=f"http_status_{status}",
                bytes_read=bytes_read,
                parse_ok=None,
                parse_error=None,
                checksum=None,
            )

        # Basic format validation only for CSV/JSON in v1
        parse_ok: Optional[bool] = None
        parse_error: Optional[str] = None

        if fmt == "csv":
            parse_ok, parse_error = try_parse_csv(content)
        elif fmt == "json":
            parse_ok, parse_error = try_parse_json(content)

        # checksum is useful for change tracking later (cheap now)
        chksum = checksum_sha256(content) if content else None

        ok = True
        if parse_ok is False:
            ok = False

        return ResourceCheckResult(
            ok=ok,
            http_status=status,
            error=None if ok else "parse_failed",
            bytes_read=bytes_read,
            parse_ok=parse_ok,
            parse_error=parse_error,
            checksum=chksum,
        )

    except Exception as e:
        return ResourceCheckResult(
            ok=False,
            http_status=None,
            error=str(e),
            bytes_read=0,
            parse_ok=None,
            parse_error=None,
            checksum=None,
        )


def dataset_last_modified(dataset: dict) -> Optional[datetime]:
    """
    Prefer dataset-level timestamps; fallback to newest resource last_modified.
    CKAN commonly provides: metadata_modified, metadata_created, or similar.
    """
    candidates = [
        dataset.get("metadata_modified"),
        dataset.get("metadata_created"),
        dataset.get("last_modified"),  # sometimes present
        dataset.get("modified"),
    ]
    for c in candidates:
        dt = safe_parse_datetime(c)
        if dt:
            return dt

    # fallback: resources
    newest: Optional[datetime] = None
    for r in dataset.get("resources", []) or []:
        dt = safe_parse_datetime(r.get("last_modified") or r.get("metadata_modified") or r.get("created"))
        if dt and (newest is None or dt > newest):
            newest = dt
    return newest


# ----------------------------
# Main
# ----------------------------
def main() -> int:
    ensure_dirs()
    now = datetime.now(timezone.utc)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "observatorio-datos-abiertos/0.1 (+https://github.com/despinoUY/observatorio-datos-abiertos)"
        }
    )

    # 1) List datasets
    print(f"[info] Fetching package_list from {action_url('package_list')}")
    packages = http_get_json(session, action_url("package_list"))["result"]
    if not isinstance(packages, list):
        raise RuntimeError("Unexpected package_list result shape")

    if MAX_DATASETS > 0:
        packages = packages[:MAX_DATASETS]

    print(f"[info] Datasets to process: {len(packages)}")

    org_agg: Dict[str, Dict[str, Any]] = {}
    datasets_out: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []

    for idx, pkg_id in enumerate(packages, start=1):
        if REQUEST_DELAY_SECONDS > 0:
            time.sleep(REQUEST_DELAY_SECONDS)

        try:
            data = http_get_json(session, action_url("package_show"), params={"id": pkg_id})
            ds = data["result"]

            ds_name = ds.get("name") or str(pkg_id)
            ds_title = ds.get("title") or ds_name

            org = ds.get("organization") or {}
            org_id = org.get("id") or "unknown"
            org_name = org.get("name") or "unknown"
            org_title = org.get("title") or org_name

            last_mod_dt = dataset_last_modified(ds)
            last_mod_iso = last_mod_dt.isoformat() if last_mod_dt else None
            d_since = days_since(last_mod_dt, now)
            bucket = freshness_bucket(d_since)

            resources = ds.get("resources", []) or []
            res_out: List[Dict[str, Any]] = []
            broken = 0
            parse_failed = 0

            for r in resources:
                r_id = r.get("id") or ""
                r_name = r.get("name") or r.get("description") or r_id
                r_url = r.get("url")
                r_fmt = (r.get("format") or "").strip()
                r_last_mod = safe_parse_datetime(r.get("last_modified") or r.get("metadata_modified"))
                r_last_mod_iso = r_last_mod.isoformat() if r_last_mod else None

                chk = check_resource(session, r)

                if not chk.ok:
                    broken += 1
                    if chk.error == "parse_failed":
                        parse_failed += 1

                res_out.append(
                    {
                        "id": r_id,
                        "name": r_name,
                        "format": r_fmt,
                        "url": r_url,
                        "last_modified": r_last_mod_iso,
                        "check": {
                            "ok": chk.ok,
                            "http_status": chk.http_status,
                            "error": chk.error,
                            "bytes_read": chk.bytes_read,
                            "parse_ok": chk.parse_ok,
                            "parse_error": chk.parse_error,
                            "checksum": chk.checksum,
                        },
                    }
                )

            ds_out = {
                "id": ds.get("id") or ds_name,
                "name": ds_name,
                "title": ds_title,
                "organization": {"id": org_id, "name": org_name, "title": org_title},
                "last_modified": last_mod_iso,
                "days_since_modified": d_since,
                "freshness_bucket": bucket,
                "resources_total": len(resources),
                "resources_broken": broken,
                "resources_parse_failed": parse_failed,
                "formats": sorted({(r.get("format") or "").strip().lower() for r in resources if r.get("format")}),
                "resources": res_out,
                "catalog_url": f"{CKAN_BASE_URL}/dataset/{ds_name}",
            }
            datasets_out.append(ds_out)

            # org aggregation
            agg = org_agg.setdefault(
                org_id,
                {
                    "id": org_id,
                    "name": org_name,
                    "title": org_title,
                    "datasets_total": 0,
                    "datasets_green": 0,
                    "datasets_yellow": 0,
                    "datasets_red": 0,
                    "datasets_unknown": 0,
                    "resources_total": 0,
                    "resources_broken": 0,
                    "resources_parse_failed": 0,
                },
            )
            agg["datasets_total"] += 1
            agg[f"datasets_{bucket}"] = agg.get(f"datasets_{bucket}", 0) + 1  # handles unknown too
            agg["resources_total"] += len(resources)
            agg["resources_broken"] += broken
            agg["resources_parse_failed"] += parse_failed

            if idx % 50 == 0:
                print(f"[info] Processed {idx}/{len(packages)} datasets...")

        except Exception as e:
            err = {"dataset": str(pkg_id), "error": str(e)}
            errors.append(err)
            # continue
            continue

    # summary
    total = len(datasets_out)
    green = sum(1 for d in datasets_out if d["freshness_bucket"] == "green")
    yellow = sum(1 for d in datasets_out if d["freshness_bucket"] == "yellow")
    red = sum(1 for d in datasets_out if d["freshness_bucket"] == "red")
    unknown = sum(1 for d in datasets_out if d["freshness_bucket"] == "unknown")
    res_total = sum(d["resources_total"] for d in datasets_out)
    res_broken = sum(d["resources_broken"] for d in datasets_out)
    res_parse_failed = sum(d["resources_parse_failed"] for d in datasets_out)

    snapshot = {
        "meta": {
            "generated_at": utc_now_iso(),
            "ckan_base_url": CKAN_BASE_URL,
            "ckan_api_path": CKAN_API_PATH,
            "freshness_thresholds_days": {"green_lt": FRESH_GREEN_DAYS, "yellow_lte": FRESH_YELLOW_DAYS},
            "note": "MVP snapshot: dataset freshness + resource availability + basic CSV/JSON parsing.",
        },
        "summary": {
            "datasets_total": total,
            "datasets_green": green,
            "datasets_yellow": yellow,
            "datasets_red": red,
            "datasets_unknown": unknown,
            "resources_total": res_total,
            "resources_broken": res_broken,
            "resources_parse_failed": res_parse_failed,
            "errors_total": len(errors),
        },
        "organizations": sorted(org_agg.values(), key=lambda x: (-(x["datasets_total"]), x["title"])),
        "datasets": datasets_out,
        "errors": errors[:500],  # keep it bounded
    }

    # write files
    latest_path = os.path.join(OUT_DIR_DATA, "latest.json")
    date_key = now.date().isoformat()
    hist_path = os.path.join(OUT_DIR_HISTORY, f"{date_key}.json")

    with open(latest_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    with open(hist_path, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)

    print(f"[ok] Wrote {latest_path}")
    print(f"[ok] Wrote {hist_path}")
    print(f"[ok] Summary: {snapshot['summary']}")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)