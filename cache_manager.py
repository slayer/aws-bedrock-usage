#!/usr/bin/env python3
"""
Cache manager for storing CloudWatch log query results with per-day granularity.
Implements two-layer caching:
- Full cache: Complete log events for reprocessing (organized by day)
- Summary cache: Pre-aggregated usage statistics for fast CSV generation (organized by day)

Per-day structure enables cache reuse across different date ranges.
"""

import hashlib
import json
import shutil
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict


# Cache directory paths
CACHE_DIR = Path(".cache")
FULL_CACHE_DIR = CACHE_DIR / "full"
SUMMARY_CACHE_DIR = CACHE_DIR / "summary"


def ensure_cache_dirs():
    """Create cache directories if they don't exist."""
    FULL_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    SUMMARY_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_log_group_hash(log_group: str) -> str:
    """
    Generate short hash from log group identifier.

    Args:
        log_group: CloudWatch log group name or S3 prefix identifier

    Returns:
        8-character hex hash
    """
    hash_obj = hashlib.md5(log_group.encode())
    return hash_obj.hexdigest()[:8]


def get_cache_dir(
    cache_type: str,
    source: str,
    region: str,
    log_group: str
) -> Path:
    """
    Get cache directory for specific source/region/log_group.

    Args:
        cache_type: "full" or "summary"
        source: Log source ("s3" or "cloudwatch")
        region: AWS region
        log_group: CloudWatch log group name or S3 prefix identifier

    Returns:
        Path to cache directory
    """
    log_group_hash = get_log_group_hash(log_group)
    if cache_type == "full":
        return FULL_CACHE_DIR / source / region / log_group_hash
    else:
        return SUMMARY_CACHE_DIR / source / region / log_group_hash


def get_day_cache_file(
    cache_type: str,
    source: str,
    region: str,
    log_group: str,
    date: str
) -> Path:
    """
    Get cache file path for a specific day.

    Args:
        cache_type: "full" or "summary"
        source: Log source ("s3" or "cloudwatch")
        region: AWS region
        log_group: CloudWatch log group name or S3 prefix identifier
        date: Date in YYYY-MM-DD format

    Returns:
        Path to cache file for this day
    """
    cache_dir = get_cache_dir(cache_type, source, region, log_group)
    return cache_dir / f"{date}.json"


def generate_date_list(start_date: str, end_date: str) -> list[str]:
    """
    Generate list of dates in range (inclusive).

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD

    Returns:
        List of YYYY-MM-DD date strings
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    return dates


def is_today(date_str: str) -> bool:
    """Check if date is today (UTC)."""
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return date_str == today


def read_full_cache_for_range(
    source: str,
    log_group: str,
    start_date: str,
    end_date: str,
    region: str
) -> tuple[list, list, dict[str, int]]:
    """
    Read full cache for date range, return events and missing dates.

    Args:
        source: Log source ("s3" or "cloudwatch")
        log_group: CloudWatch log group name or S3 prefix identifier
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        region: AWS region

    Returns:
        (events, missing_dates, last_fetch_timestamps)
        - events: List of all cached events for available dates
        - missing_dates: List of YYYY-MM-DD dates not in cache or incomplete (today)
        - last_fetch_timestamps: Dict mapping date to last fetch timestamp (for today)
    """
    cache_dir = get_cache_dir("full", source, region, log_group)
    all_dates = generate_date_list(start_date, end_date)

    events = []
    missing_dates = []
    last_fetch_timestamps = {}

    for date in all_dates:
        cache_file = cache_dir / f"{date}.json"

        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)

                # Load existing events
                events.extend(data.get("events", []))

                # If this is today, get last fetch timestamp and mark as missing for incremental update
                if is_today(date):
                    last_fetch_ts = data.get("metadata", {}).get("last_fetch_timestamp")
                    if last_fetch_ts:
                        last_fetch_timestamps[date] = last_fetch_ts
                    # Always mark today as "missing" to trigger incremental update
                    missing_dates.append(date)

            except (json.JSONDecodeError, IOError, KeyError):
                missing_dates.append(date)
        else:
            missing_dates.append(date)

    return events, missing_dates, last_fetch_timestamps


def write_full_cache_by_day(
    source: str,
    log_group: str,
    region: str,
    events_by_day: dict[str, list],
    append_mode: dict[str, bool] = None
):
    """
    Write full cache organized by day.

    Args:
        source: Log source ("s3" or "cloudwatch")
        log_group: CloudWatch log group name or S3 prefix identifier
        region: AWS region
        events_by_day: Dict mapping YYYY-MM-DD to list of events for that day
        append_mode: Dict mapping YYYY-MM-DD to bool (True = append to existing cache)
    """
    cache_dir = get_cache_dir("full", source, region, log_group)
    cache_dir.mkdir(parents=True, exist_ok=True)

    if append_mode is None:
        append_mode = {}

    for date, events in events_by_day.items():
        cache_file = cache_dir / f"{date}.json"

        # For append mode (today's incremental update)
        if append_mode.get(date, False) and cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    existing_data = json.load(f)
                existing_events = existing_data.get("events", [])
                # Append new events to existing
                all_events = existing_events + events
            except (json.JSONDecodeError, IOError, KeyError):
                all_events = events
        else:
            all_events = events

        cache_data = {
            "metadata": {
                "source": source,
                "log_group": log_group,
                "date": date,
                "region": region,
                "cached_at": datetime.now(timezone.utc).isoformat(),
                "event_count": len(all_events),
                "last_fetch_timestamp": int(datetime.now(timezone.utc).timestamp() * 1000)
            },
            "events": all_events
        }

        with open(cache_file, "w") as f:
            json.dump(cache_data, f, indent=2)


def read_summary_cache_for_range(
    source: str,
    log_group: str,
    start_date: str,
    end_date: str,
    region: str
) -> tuple[dict, list]:
    """
    Read summary cache for date range, merge usage data.

    Args:
        source: Log source ("s3" or "cloudwatch")
        log_group: CloudWatch log group name or S3 prefix identifier
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD
        region: AWS region

    Returns:
        (usage, missing_dates)
        - usage: Merged usage dict from all cached days
        - missing_dates: List of YYYY-MM-DD dates not in cache
    """
    cache_dir = get_cache_dir("summary", source, region, log_group)
    all_dates = generate_date_list(start_date, end_date)

    merged_usage = {}
    missing_dates = []

    for date in all_dates:
        cache_file = cache_dir / f"{date}.json"

        # Always treat today as missing to get latest data
        if is_today(date):
            missing_dates.append(date)
            continue

        if cache_file.exists():
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                day_usage = data.get("usage", {})

                # Convert serialized data back to proper structure
                for arn, arn_data in day_usage.items():
                    # Handle both old and new cache format
                    if "totals" in arn_data:
                        # New format with per-model breakdown
                        arn_data["totals"]["models_used"] = set(arn_data["totals"]["models_used"])
                    else:
                        # Old format - skip or convert (skip for now)
                        continue

                # Merge into accumulated usage
                merged_usage = merge_usage_dicts(merged_usage, day_usage)
            except (json.JSONDecodeError, IOError, KeyError):
                missing_dates.append(date)
        else:
            missing_dates.append(date)

    return merged_usage, missing_dates


def write_summary_cache_by_day(
    source: str,
    log_group: str,
    region: str,
    usage_by_day: dict[str, dict]
):
    """
    Write summary cache organized by day.

    Args:
        source: Log source ("s3" or "cloudwatch")
        log_group: CloudWatch log group name or S3 prefix identifier
        region: AWS region
        usage_by_day: Dict mapping YYYY-MM-DD to usage dict for that day
    """
    cache_dir = get_cache_dir("summary", source, region, log_group)
    cache_dir.mkdir(parents=True, exist_ok=True)

    for date, usage in usage_by_day.items():
        cache_file = cache_dir / f"{date}.json"

        # Convert to JSON-serializable format
        usage_serializable = {}
        for arn, arn_data in usage.items():
            # Convert models dict (which may be defaultdict) to regular dict
            models_dict = dict(arn_data["models"])

            # Convert totals and models_used set to list
            totals = arn_data["totals"].copy()
            totals["models_used"] = sorted(list(totals["models_used"]))

            usage_serializable[arn] = {
                "models": models_dict,
                "totals": totals
            }

        cache_data = {
            "metadata": {
                "source": source,
                "log_group": log_group,
                "date": date,
                "region": region,
                "cached_at": datetime.now(timezone.utc).isoformat()
            },
            "usage": usage_serializable
        }

        with open(cache_file, "w") as f:
            json.dump(cache_data, f, indent=2)


def merge_usage_dicts(usage1: dict, usage2: dict) -> dict:
    """
    Merge two usage dicts with per-model breakdown.

    Args:
        usage1: First usage dict
        usage2: Second usage dict

    Returns:
        Merged usage dict with summed metrics
    """
    result = {}

    # Copy usage1 data
    for arn, arn_data in usage1.items():
        result[arn] = {
            "models": defaultdict(
                lambda: {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_tokens": 0,
                    "cache_write_tokens": 0,
                    "request_count": 0,
                }
            ),
            "totals": arn_data["totals"].copy()
        }
        # Ensure models_used is a set
        result[arn]["totals"]["models_used"] = set(arn_data["totals"]["models_used"])

        # Copy per-model data
        for model_id, model_metrics in arn_data["models"].items():
            result[arn]["models"][model_id] = model_metrics.copy()

    # Merge usage2 into result
    for arn, arn_data in usage2.items():
        if arn not in result:
            result[arn] = {
                "models": defaultdict(
                    lambda: {
                        "input_tokens": 0,
                        "output_tokens": 0,
                        "cache_read_tokens": 0,
                        "cache_write_tokens": 0,
                        "request_count": 0,
                    }
                ),
                "totals": arn_data["totals"].copy()
            }
            result[arn]["totals"]["models_used"] = set(arn_data["totals"]["models_used"])

            # Copy per-model data
            for model_id, model_metrics in arn_data["models"].items():
                result[arn]["models"][model_id] = model_metrics.copy()
        else:
            # Merge per-model stats
            for model_id, model_metrics in arn_data["models"].items():
                if model_id in result[arn]["models"]:
                    result[arn]["models"][model_id]["input_tokens"] += model_metrics["input_tokens"]
                    result[arn]["models"][model_id]["output_tokens"] += model_metrics["output_tokens"]
                    result[arn]["models"][model_id]["cache_read_tokens"] += model_metrics["cache_read_tokens"]
                    result[arn]["models"][model_id]["cache_write_tokens"] += model_metrics["cache_write_tokens"]
                    result[arn]["models"][model_id]["request_count"] += model_metrics["request_count"]
                else:
                    result[arn]["models"][model_id] = model_metrics.copy()

            # Merge totals
            result[arn]["totals"]["total_input_tokens"] += arn_data["totals"]["total_input_tokens"]
            result[arn]["totals"]["total_output_tokens"] += arn_data["totals"]["total_output_tokens"]
            result[arn]["totals"]["cache_read_tokens"] += arn_data["totals"]["cache_read_tokens"]
            result[arn]["totals"]["cache_write_tokens"] += arn_data["totals"]["cache_write_tokens"]
            result[arn]["totals"]["request_count"] += arn_data["totals"]["request_count"]
            result[arn]["totals"]["models_used"] = result[arn]["totals"]["models_used"].union(
                set(arn_data["totals"]["models_used"])
            )

    return result


def clear_full_cache():
    """Remove all files from full cache directory."""
    if not FULL_CACHE_DIR.exists():
        print("Full cache directory doesn't exist - nothing to clear")
        return

    # Count all JSON files recursively
    files = list(FULL_CACHE_DIR.rglob("*.json"))
    count = len(files)

    for file in files:
        file.unlink()

    # Remove empty directories
    for dirpath in sorted(FULL_CACHE_DIR.rglob("*"), reverse=True):
        if dirpath.is_dir() and not any(dirpath.iterdir()):
            dirpath.rmdir()

    print(f"Cleared {count} file(s) from full cache")


def clear_all_cache():
    """Remove entire cache directory."""
    if not CACHE_DIR.exists():
        print("Cache directory doesn't exist - nothing to clear")
        return

    # Count files before clearing
    full_count = len(list(FULL_CACHE_DIR.rglob("*.json"))) if FULL_CACHE_DIR.exists() else 0
    summary_count = len(list(SUMMARY_CACHE_DIR.rglob("*.json"))) if SUMMARY_CACHE_DIR.exists() else 0

    shutil.rmtree(CACHE_DIR)
    print(f"Cleared all cache: {full_count} full cache files, {summary_count} summary cache files")


# Create cache directories on module import
ensure_cache_dirs()
