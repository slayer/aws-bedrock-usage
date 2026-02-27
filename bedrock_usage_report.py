#!/usr/bin/env python3
"""
CLI tool to query AWS CloudWatch logs from BedrockLogging and generate
a CSV report of Bedrock usage per user (identified by IAM ARN).
"""

import argparse
import csv
import json
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

# CloudWatch clients are not thread-safe; each thread gets its own
_cw_thread_local = threading.local()

# ANSI color codes for terminal output
YELLOW = '\033[93m'
RESET = '\033[0m'

from cache_manager import (
    read_full_cache_for_range,
    write_full_cache_by_day,
    read_summary_cache_for_range,
    write_summary_cache_by_day,
    clear_full_cache,
    clear_all_cache,
    generate_date_list,
)
from s3_log_source import query_s3_logs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Generate Bedrock usage report from S3 or CloudWatch logs"
    )
    parser.add_argument(
        "--source",
        default="s3",
        choices=["s3", "cloudwatch", "both"],
        help="Log source: s3 (default), cloudwatch, or both",
    )
    parser.add_argument(
        "--log-group",
        default="BedrockModelInvocationLogging",
        help="CloudWatch log group name (default: BedrockModelInvocationLogging)",
    )
    parser.add_argument(
        "--s3-bucket",
        default="your-bedrock-logs-bucket",
        help="S3 bucket name for Bedrock logs",
    )
    parser.add_argument(
        "--s3-prefix",
        default="AWSLogs/123456789012/BedrockModelInvocationLogs",
        help="S3 prefix for Bedrock logs (default: AWSLogs/<account-id>/BedrockModelInvocationLogs)",
    )
    parser.add_argument(
        "--start-date",
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--profile",
        default=None,
        help="AWS profile name (default: use default credentials)",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region (default: us-east-1)",
    )
    parser.add_argument(
        "--csv",
        action="store_true",
        help="Write output to CSV file (default: print ASCII table to stdout)",
    )
    parser.add_argument(
        "--output",
        default="usage_report.csv",
        help="CSV file path when --csv is used (default: usage_report.csv). Filename will be prefixed with date range.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Skip cache and query data sources directly",
    )
    parser.add_argument(
        "--clear-full-cache",
        action="store_true",
        help="Clear full cache (keeps summary cache) and exit",
    )
    parser.add_argument(
        "--clear-all-cache",
        action="store_true",
        help="Clear all cache (full and summary) and exit",
    )
    parser.add_argument(
        "--show-costs",
        action="store_true",
        default=True,
        help="Enable cost calculation and display (default: enabled, requires pricing data)",
    )
    parser.add_argument(
        "--no-costs",
        action="store_false",
        dest="show_costs",
        help="Disable cost calculation and display",
    )
    parser.add_argument(
        "--pricing-region",
        default="us-east-1",
        choices=["us-east-1", "eu-central-1", "ap-south-1"],
        help="AWS region for Pricing API queries (default: us-east-1)",
    )
    parser.add_argument(
        "--refresh-pricing",
        action="store_true",
        help="Force refresh pricing cache from AWS Pricing API",
    )
    parser.add_argument(
        "--pricing-cache-ttl",
        type=int,
        default=24,
        help="Pricing cache TTL in hours (default: 24)",
    )
    parser.add_argument(
        "--clear-pricing-cache",
        action="store_true",
        help="Clear pricing cache and exit",
    )
    parser.add_argument(
        "--summary-only",
        action="store_true",
        help="Show only per-user totals without per-model breakdown (default: show detailed per-model breakdown)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel threads for S3 and CloudWatch queries (default: 10)",
    )
    return parser.parse_args()


def parse_date_to_epoch_ms(date_str: str, end_of_day: bool = False) -> int:
    """Convert YYYY-MM-DD to epoch milliseconds."""
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_of_day:
        # Set to 23:59:59.999
        dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
    return int(dt.timestamp() * 1000)


def extract_username(arn: str) -> str:
    """Extract username from IAM ARN."""
    # ARN format: arn:aws:iam::ACCOUNT:user/path/username
    # or: arn:aws:sts::ACCOUNT:assumed-role/role-name/session-name
    if "/user/" in arn:
        return arn.split("/")[-1]
    elif "/assumed-role/" in arn:
        parts = arn.split("/")
        return f"{parts[-2]}/{parts[-1]}"  # role-name/session-name
    return arn.split("/")[-1]


def extract_model_name(model_arn: str) -> str:
    """
    Extract human-readable model name from Bedrock model ARN.

    Examples:
        arn:aws:bedrock:us-east-1:123:inference-profile/us.anthropic.claude-sonnet-4-5-20250929-v1:0
        -> Claude Sonnet 4.5

        arn:aws:bedrock:region:account:inference-profile/global.anthropic.claude-opus-4-5-20251101-v1:0
        -> Claude Opus 4.5

        arn:aws:bedrock:us-east-1:123:inference-profile/us.anthropic.claude-3-5-haiku-20241022-v1:0
        -> Claude 3.5 Haiku
    """
    # Extract the model ID from ARN
    if "/inference-profile/" in model_arn:
        model_id = model_arn.split("/inference-profile/")[-1]
    else:
        model_id = model_arn.split("/")[-1] if "/" in model_arn else model_arn

    # Remove region prefix (us., global., etc.) and provider (anthropic.)
    model_id = model_id.split(".")[-1] if "." in model_id else model_id

    # Parse Claude model names
    if "claude" in model_id.lower():
        # Extract model variant and version
        parts = model_id.lower().replace("claude-", "").split("-")

        # Handle different naming patterns
        if len(parts) >= 2:
            # Check for version number patterns like "3-5" or "4-5"
            if parts[0].isdigit() and len(parts[0]) == 1 and parts[1].isdigit() and len(parts[1]) == 1:
                # Format: claude-3-5-haiku or claude-4-5
                version = f"{parts[0]}.{parts[1]}"
                variant = parts[2].title() if len(parts) > 2 and parts[2].isalpha() else ""
                if variant:
                    return f"Claude {version} {variant}"
                else:
                    return f"Claude {version}"
            elif parts[0].isalpha():
                # Format: claude-sonnet-4-5 or claude-opus-4-5 or claude-sonnet-4-20250514
                variant = parts[0].title()
                if len(parts) >= 3 and parts[1].isdigit() and len(parts[1]) == 1 and parts[2].isdigit() and len(parts[2]) == 1:
                    # Version format: 4-5
                    version = f"{parts[1]}.{parts[2]}"
                    return f"Claude {variant} {version}"
                elif len(parts) >= 2 and parts[1].isdigit():
                    # Version format: 4 or 4-20250514 (just use major version)
                    version = parts[1]
                    return f"Claude {variant} {version}"
                else:
                    return f"Claude {variant}"

    # Fallback: return the model ID with basic cleanup
    return model_id.replace("-", " ").title()


def _query_logs_for_day(session, log_group: str, start_ms: int, end_ms: int, date_label: str) -> list:
    """Query CloudWatch Logs for a single day. Uses thread-local client."""
    import threading as _threading
    if not hasattr(_cw_thread_local, 'client'):
        _cw_thread_local.client = session.client('logs')
    client = _cw_thread_local.client

    events = []
    next_token = None
    page_count = 0

    while True:
        page_count += 1
        kwargs = {
            "logGroupName": log_group,
            "startTime": start_ms,
            "endTime": end_ms,
            "filterPattern": "inputTokenCount",
        }
        if next_token:
            kwargs["nextToken"] = next_token

        response = client.filter_log_events(**kwargs)
        page_events = response.get("events", [])
        events.extend(page_events)

        next_token = response.get("nextToken")
        if not next_token:
            break

    if events:
        print(f"  {date_label}: {len(events)} events ({page_count} pages)")
    return events


def query_logs(session, log_group: str, start_ms: int, end_ms: int,
               start_date: str = None, end_date: str = None,
               max_workers: int = 10) -> list:
    """
    Query CloudWatch Logs, parallelized by day.

    Each day in the range gets its own filter_log_events pagination chain
    running in a separate thread. Thread-local clients for safety.

    Args:
        session: boto3 Session (NOT a client)
        log_group: CloudWatch log group name
        start_ms: Start time epoch ms (used as fallback if dates not provided)
        end_ms: End time epoch ms (used as fallback if dates not provided)
        start_date: Start date YYYY-MM-DD (enables per-day parallelism)
        end_date: End date YYYY-MM-DD (enables per-day parallelism)
        max_workers: Thread pool size
    """
    from botocore.exceptions import ClientError
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    print(f"Querying CloudWatch Logs from {log_group}...")

    # If no date strings provided, fall back to single sequential query
    if not start_date or not end_date:
        return _query_logs_for_day(session, log_group, start_ms, end_ms, "all")

    dates = generate_date_list(start_date, end_date)
    print(f"  Querying {len(dates)} days in parallel (workers: {max_workers})")

    all_events = []
    lock = threading.Lock()

    def _day_task(date_str):
        day_start = parse_date_to_epoch_ms(date_str)
        day_end = parse_date_to_epoch_ms(date_str, end_of_day=True)
        # Respect original bounds (incremental updates may shift start_ms)
        effective_start = max(day_start, start_ms)
        effective_end = min(day_end, end_ms)
        if effective_start > effective_end:
            return []
        return _query_logs_for_day(session, log_group, effective_start, effective_end, date_str)

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_day_task, d): d for d in dates}
            for future in as_completed(futures):
                date = futures[future]
                try:
                    events = future.result()
                    if events:
                        with lock:
                            all_events.extend(events)
                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "")
                    if error_code == "AccessDeniedException":
                        print(f"\nERROR: Access denied. Your IAM user/role needs these permissions:")
                        print("  - logs:FilterLogEvents")
                        print(f"  on resource: arn:aws:logs:*:*:log-group:{log_group}:*")
                        print("\nTry using a different --profile with appropriate permissions.")
                        sys.exit(1)
                    raise
                except Exception as exc:
                    print(f"  Warning: query failed for {date}: {exc}")
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "AccessDeniedException":
            print(f"\nERROR: Access denied. Your IAM user/role needs these permissions:")
            print("  - logs:FilterLogEvents")
            print(f"  on resource: arn:aws:logs:*:*:log-group:{log_group}:*")
            print("\nTry using a different --profile with appropriate permissions.")
            sys.exit(1)
        raise

    print(f"Query complete. Found {len(all_events)} log entries.")
    return all_events


def process_log_entry(message: str) -> dict | None:
    """Parse a log message and extract relevant fields."""
    try:
        data = json.loads(message)
    except json.JSONDecodeError:
        return None

    identity = data.get("identity", {})
    arn = identity.get("arn", "unknown")

    input_data = data.get("input", {})
    output_data = data.get("output", {})

    return {
        "arn": arn,
        "model_id": data.get("modelId", "unknown"),
        "input_tokens": input_data.get("inputTokenCount", 0),
        "output_tokens": output_data.get("outputTokenCount", 0),
        "cache_read_tokens": input_data.get("cacheReadInputTokenCount", 0),
        "cache_write_tokens": input_data.get("cacheWriteInputTokenCount", 0),
    }


def aggregate_usage(log_results: list) -> dict:
    """
    Aggregate usage statistics by user ARN and model.

    Returns structure:
    {
        arn: {
            "models": {
                model_id: {
                    "input_tokens": int,
                    "output_tokens": int,
                    "cache_read_tokens": int,
                    "cache_write_tokens": int,
                    "request_count": int
                }
            },
            "totals": {
                "total_input_tokens": int,
                "total_output_tokens": int,
                "cache_read_tokens": int,
                "cache_write_tokens": int,
                "request_count": int,
                "models_used": set
            }
        }
    }
    """
    usage = defaultdict(
        lambda: {
            "models": defaultdict(
                lambda: {
                    "input_tokens": 0,
                    "output_tokens": 0,
                    "cache_read_tokens": 0,
                    "cache_write_tokens": 0,
                    "request_count": 0,
                }
            ),
            "totals": {
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "request_count": 0,
                "models_used": set(),
            }
        }
    )

    for event in log_results:
        message = event.get("message")
        if not message:
            continue

        entry = process_log_entry(message)
        if not entry:
            continue

        arn = entry["arn"]
        model_id = entry["model_id"]

        # Update per-model stats
        usage[arn]["models"][model_id]["input_tokens"] += entry["input_tokens"]
        usage[arn]["models"][model_id]["output_tokens"] += entry["output_tokens"]
        usage[arn]["models"][model_id]["cache_read_tokens"] += entry["cache_read_tokens"]
        usage[arn]["models"][model_id]["cache_write_tokens"] += entry["cache_write_tokens"]
        usage[arn]["models"][model_id]["request_count"] += 1

        # Update totals
        usage[arn]["totals"]["total_input_tokens"] += entry["input_tokens"]
        usage[arn]["totals"]["total_output_tokens"] += entry["output_tokens"]
        usage[arn]["totals"]["cache_read_tokens"] += entry["cache_read_tokens"]
        usage[arn]["totals"]["cache_write_tokens"] += entry["cache_write_tokens"]
        usage[arn]["totals"]["request_count"] += 1
        usage[arn]["totals"]["models_used"].add(model_id)

    return usage


def merge_usage(usage1: dict, usage2: dict) -> dict:
    """
    Merge two usage dictionaries (for combining cloudwatch + s3).

    Args:
        usage1: First usage dict (or empty)
        usage2: Second usage dict

    Returns:
        Merged usage dict with summed metrics
    """
    result = {}

    # Copy usage1 data
    for arn, data in usage1.items():
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
            "totals": data["totals"].copy()
        }
        # Ensure models_used is a set
        result[arn]["totals"]["models_used"] = set(data["totals"]["models_used"])

        # Copy per-model data
        for model_id, model_metrics in data["models"].items():
            result[arn]["models"][model_id] = model_metrics.copy()

    # Merge usage2 into result
    for arn, data in usage2.items():
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
                "totals": data["totals"].copy()
            }
            result[arn]["totals"]["models_used"] = set(data["totals"]["models_used"])

            # Copy per-model data
            for model_id, model_metrics in data["models"].items():
                result[arn]["models"][model_id] = model_metrics.copy()
        else:
            # Merge per-model stats
            for model_id, model_metrics in data["models"].items():
                if model_id in result[arn]["models"]:
                    result[arn]["models"][model_id]["input_tokens"] += model_metrics["input_tokens"]
                    result[arn]["models"][model_id]["output_tokens"] += model_metrics["output_tokens"]
                    result[arn]["models"][model_id]["cache_read_tokens"] += model_metrics["cache_read_tokens"]
                    result[arn]["models"][model_id]["cache_write_tokens"] += model_metrics["cache_write_tokens"]
                    result[arn]["models"][model_id]["request_count"] += model_metrics["request_count"]
                else:
                    result[arn]["models"][model_id] = model_metrics.copy()

            # Merge totals
            result[arn]["totals"]["total_input_tokens"] += data["totals"]["total_input_tokens"]
            result[arn]["totals"]["total_output_tokens"] += data["totals"]["total_output_tokens"]
            result[arn]["totals"]["cache_read_tokens"] += data["totals"]["cache_read_tokens"]
            result[arn]["totals"]["cache_write_tokens"] += data["totals"]["cache_write_tokens"]
            result[arn]["totals"]["request_count"] += data["totals"]["request_count"]
            result[arn]["totals"]["models_used"] = result[arn]["totals"]["models_used"].union(
                set(data["totals"]["models_used"])
            )

    return result


def split_events_by_day(events: list) -> dict[str, list]:
    """
    Split log events by day based on timestamp.

    Args:
        events: List of events with 'timestamp' field (epoch ms)

    Returns:
        Dict mapping YYYY-MM-DD to list of events for that day
    """
    events_by_day = defaultdict(list)

    for event in events:
        timestamp = event.get("timestamp", 0)
        if timestamp:
            dt = datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")
            events_by_day[date_str].append(event)

    return dict(events_by_day)


def format_ascii_table(usage: dict, show_costs: bool = False) -> str:
    """
    Format usage data as ASCII table with Unicode box-drawing characters.

    Args:
        usage: Dict mapping ARN to usage metrics (with per-model breakdown)
        show_costs: Whether to include cost column

    Returns:
        Formatted table string
    """
    from textwrap import shorten

    if not usage:
        return "No usage data found."

    # Prepare rows - show per-user per-model breakdown
    rows = []
    for arn in sorted(usage.keys()):
        data = usage[arn]
        username = extract_username(arn)

        # Add rows for each model used by this user
        for model_id in sorted(data["models"].keys()):
            model_metrics = data["models"][model_id]
            model_name = extract_model_name(model_id)

            row = {
                "row_type": "detail",
                "username": username,
                "user_arn": arn,
                "model": model_name,
                "input_tokens": model_metrics["input_tokens"],
                "output_tokens": model_metrics["output_tokens"],
                "cache_read_tokens": model_metrics["cache_read_tokens"],
                "cache_write_tokens": model_metrics["cache_write_tokens"],
                "request_count": model_metrics["request_count"],
            }

            if show_costs and "costs" in model_metrics:
                costs = model_metrics["costs"]
                row["total_cost"] = f"${costs['total_cost']:.2f}"

            rows.append(row)

        # Add summary row for this user (only if costs are shown)
        if show_costs and "costs" in data["totals"]:
            totals = data["totals"]
            total_costs = totals["costs"]

            summary_row = {
                "row_type": "summary",
                "username": "TOTAL",
                "user_arn": arn,
                "model": "All Models",
                "input_tokens": totals["total_input_tokens"],
                "output_tokens": totals["total_output_tokens"],
                "cache_read_tokens": totals["cache_read_tokens"],
                "cache_write_tokens": totals["cache_write_tokens"],
                "request_count": totals["request_count"],
                "total_cost": f"${total_costs['total_cost']:.2f}"
            }

            rows.append(summary_row)

    # Add grand total row (only if costs are shown and there are multiple users)
    if show_costs and len(usage) > 0:
        grand_total = {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "request_count": 0,
            "total_cost": 0.0
        }

        for arn in usage.keys():
            data = usage[arn]
            if "costs" in data["totals"]:
                totals = data["totals"]
                total_costs = totals["costs"]
                grand_total["total_input_tokens"] += totals["total_input_tokens"]
                grand_total["total_output_tokens"] += totals["total_output_tokens"]
                grand_total["cache_read_tokens"] += totals["cache_read_tokens"]
                grand_total["cache_write_tokens"] += totals["cache_write_tokens"]
                grand_total["request_count"] += totals["request_count"]
                grand_total["total_cost"] += total_costs["total_cost"]

        grand_total_row = {
            "row_type": "grand_total",
            "username": "GRAND TOTAL",
            "user_arn": "",
            "model": "All Users, All Models",
            "input_tokens": grand_total["total_input_tokens"],
            "output_tokens": grand_total["total_output_tokens"],
            "cache_read_tokens": grand_total["cache_read_tokens"],
            "cache_write_tokens": grand_total["cache_write_tokens"],
            "request_count": grand_total["request_count"],
            "total_cost": f"${grand_total['total_cost']:.2f}"
        }

        rows.append(grand_total_row)

    # Calculate column widths (excluding user_arn for table output)
    col_widths = {
        "username": max(len("Username"), max(len(r["username"]) for r in rows)),
        "model": max(len("Model"), max(len(r["model"]) for r in rows)),
        "input_tokens": max(len("Input Tokens"), max(len(str(r["input_tokens"])) for r in rows)),
        "output_tokens": max(len("Output Tokens"), max(len(str(r["output_tokens"])) for r in rows)),
        "cache_read_tokens": max(len("Cache Read"), max(len(str(r["cache_read_tokens"])) for r in rows)),
        "cache_write_tokens": max(len("Cache Write"), max(len(str(r["cache_write_tokens"])) for r in rows)),
        "request_count": max(len("Requests"), max(len(str(r["request_count"])) for r in rows)),
    }

    if show_costs:
        col_widths["total_cost"] = max(
            len("Total Cost"),
            max(len(r.get("total_cost", "N/A")) for r in rows)
        )

    # Limit model column for readability
    col_widths["model"] = min(col_widths["model"], 40)

    # Build table with Unicode box-drawing characters
    table_parts = []

    # Top border
    table_parts.append("┌" + "┬".join("─" * (w + 2) for w in col_widths.values()) + "┐")

    # Header row
    header_cells = [
        "Username".ljust(col_widths["username"]),
        "Model".ljust(col_widths["model"]),
        "Input Tokens".rjust(col_widths["input_tokens"]),
        "Output Tokens".rjust(col_widths["output_tokens"]),
        "Cache Read".rjust(col_widths["cache_read_tokens"]),
        "Cache Write".rjust(col_widths["cache_write_tokens"]),
        "Requests".rjust(col_widths["request_count"]),
    ]
    if show_costs:
        header_cells.append("Total Cost".rjust(col_widths["total_cost"]))
    table_parts.append("│ " + " │ ".join(header_cells) + " │")

    # Header separator
    table_parts.append("├" + "┼".join("─" * (w + 2) for w in col_widths.values()) + "┤")

    # Data rows
    prev_username = None
    prev_user_arn = None
    for row in rows:
        # Add separator before grand total row
        if row["row_type"] == "grand_total":
            table_parts.append("├" + "┼".join("─" * (w + 2) for w in col_widths.values()) + "┤")

        # Truncate long values
        model_display = shorten(row["model"], width=col_widths["model"], placeholder="...")

        # Show username only for first model per user (detail rows)
        # Always show for summary and grand_total rows
        if row["row_type"] in ("summary", "grand_total"):
            username_display = row["username"]
        elif row["user_arn"] == prev_user_arn:
            username_display = ""
        else:
            username_display = row["username"]
            prev_user_arn = row["user_arn"]

        data_cells = [
            username_display.ljust(col_widths["username"]),
            model_display.ljust(col_widths["model"]),
            str(row["input_tokens"]).rjust(col_widths["input_tokens"]),
            str(row["output_tokens"]).rjust(col_widths["output_tokens"]),
            str(row["cache_read_tokens"]).rjust(col_widths["cache_read_tokens"]),
            str(row["cache_write_tokens"]).rjust(col_widths["cache_write_tokens"]),
            str(row["request_count"]).rjust(col_widths["request_count"]),
        ]
        if show_costs:
            data_cells.append(
                row.get("total_cost", "N/A").rjust(col_widths["total_cost"])
            )

        # Build row string
        row_str = "│ " + " │ ".join(data_cells) + " │"

        # Apply yellow color to summary and grand_total rows
        if row["row_type"] in ("summary", "grand_total"):
            row_str = f"{YELLOW}{row_str}{RESET}"

        table_parts.append(row_str)

        # Add separator after summary rows (except last row or if next is grand_total)
        if row["row_type"] == "summary" and row != rows[-1]:
            # Check if next row is grand_total
            row_idx = rows.index(row)
            if row_idx + 1 < len(rows) and rows[row_idx + 1]["row_type"] != "grand_total":
                table_parts.append("├" + "┼".join("─" * (w + 2) for w in col_widths.values()) + "┤")

    # Bottom border
    table_parts.append("└" + "┴".join("─" * (w + 2) for w in col_widths.values()) + "┘")

    return "\n".join(table_parts)


def format_ascii_table_summary_only(usage: dict, show_costs: bool = False) -> str:
    """
    Format usage data as ASCII table showing only per-user totals (no per-model breakdown).

    Args:
        usage: Dict mapping ARN to usage metrics (with per-model breakdown)
        show_costs: Whether to include cost column

    Returns:
        Formatted table string with one row per user + grand total
    """
    from textwrap import shorten

    if not usage:
        return "No usage data found."

    # Prepare rows - one row per user (using totals only)
    rows = []
    for arn in sorted(usage.keys()):
        data = usage[arn]
        username = extract_username(arn)
        totals = data["totals"]

        # Get comma-separated list of model names
        model_names = [extract_model_name(model_id) for model_id in sorted(data["models"].keys())]
        models_display = ", ".join(model_names)

        row = {
            "row_type": "user_total",
            "username": username,
            "user_arn": arn,
            "models": models_display,
            "input_tokens": totals["total_input_tokens"],
            "output_tokens": totals["total_output_tokens"],
            "cache_read_tokens": totals["cache_read_tokens"],
            "cache_write_tokens": totals["cache_write_tokens"],
            "request_count": totals["request_count"],
        }

        if show_costs and "costs" in totals:
            total_costs = totals["costs"]
            row["total_cost"] = f"${total_costs['total_cost']:.2f}"

        rows.append(row)

    # Add grand total row (only if costs are shown)
    if show_costs and len(usage) > 0:
        grand_total = {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "request_count": 0,
            "total_cost": 0.0
        }

        for arn in usage.keys():
            data = usage[arn]
            if "costs" in data["totals"]:
                totals = data["totals"]
                total_costs = totals["costs"]
                grand_total["total_input_tokens"] += totals["total_input_tokens"]
                grand_total["total_output_tokens"] += totals["total_output_tokens"]
                grand_total["cache_read_tokens"] += totals["cache_read_tokens"]
                grand_total["cache_write_tokens"] += totals["cache_write_tokens"]
                grand_total["request_count"] += totals["request_count"]
                grand_total["total_cost"] += total_costs["total_cost"]

        grand_total_row = {
            "row_type": "grand_total",
            "username": "GRAND TOTAL",
            "user_arn": "",
            "models": "All Users, All Models",
            "input_tokens": grand_total["total_input_tokens"],
            "output_tokens": grand_total["total_output_tokens"],
            "cache_read_tokens": grand_total["cache_read_tokens"],
            "cache_write_tokens": grand_total["cache_write_tokens"],
            "request_count": grand_total["request_count"],
            "total_cost": f"${grand_total['total_cost']:.2f}"
        }

        rows.append(grand_total_row)

    # Calculate column widths (models column shows comma-separated list)
    col_widths = {
        "username": max(len("Username"), max(len(r["username"]) for r in rows)),
        "models": max(len("Models"), max(len(r["models"]) for r in rows)),
        "input_tokens": max(len("Input Tokens"), max(len(str(r["input_tokens"])) for r in rows)),
        "output_tokens": max(len("Output Tokens"), max(len(str(r["output_tokens"])) for r in rows)),
        "cache_read_tokens": max(len("Cache Read"), max(len(str(r["cache_read_tokens"])) for r in rows)),
        "cache_write_tokens": max(len("Cache Write"), max(len(str(r["cache_write_tokens"])) for r in rows)),
        "request_count": max(len("Requests"), max(len(str(r["request_count"])) for r in rows)),
    }

    # Limit models column for readability
    col_widths["models"] = min(col_widths["models"], 60)

    if show_costs:
        col_widths["total_cost"] = max(
            len("Total Cost"),
            max(len(r.get("total_cost", "N/A")) for r in rows)
        )

    # Build table with Unicode box-drawing characters
    table_parts = []

    # Top border
    table_parts.append("┌" + "┬".join("─" * (w + 2) for w in col_widths.values()) + "┐")

    # Header row (includes Models column)
    header_cells = [
        "Username".ljust(col_widths["username"]),
        "Models".ljust(col_widths["models"]),
        "Input Tokens".rjust(col_widths["input_tokens"]),
        "Output Tokens".rjust(col_widths["output_tokens"]),
        "Cache Read".rjust(col_widths["cache_read_tokens"]),
        "Cache Write".rjust(col_widths["cache_write_tokens"]),
        "Requests".rjust(col_widths["request_count"]),
    ]
    if show_costs:
        header_cells.append("Total Cost".rjust(col_widths["total_cost"]))
    table_parts.append("│ " + " │ ".join(header_cells) + " │")

    # Header separator
    table_parts.append("├" + "┼".join("─" * (w + 2) for w in col_widths.values()) + "┤")

    # Data rows
    for row in rows:
        # Add separator before grand total
        if row["row_type"] == "grand_total":
            table_parts.append("├" + "┼".join("─" * (w + 2) for w in col_widths.values()) + "┤")

        # Truncate models if too long
        models_display = shorten(row["models"], width=col_widths["models"], placeholder="...")

        data_cells = [
            row["username"].ljust(col_widths["username"]),
            models_display.ljust(col_widths["models"]),
            str(row["input_tokens"]).rjust(col_widths["input_tokens"]),
            str(row["output_tokens"]).rjust(col_widths["output_tokens"]),
            str(row["cache_read_tokens"]).rjust(col_widths["cache_read_tokens"]),
            str(row["cache_write_tokens"]).rjust(col_widths["cache_write_tokens"]),
            str(row["request_count"]).rjust(col_widths["request_count"]),
        ]
        if show_costs:
            data_cells.append(
                row.get("total_cost", "N/A").rjust(col_widths["total_cost"])
            )

        # Build row string
        row_str = "│ " + " │ ".join(data_cells) + " │"

        # Apply yellow color to grand total row
        if row["row_type"] == "grand_total":
            row_str = f"{YELLOW}{row_str}{RESET}"

        table_parts.append(row_str)

    # Bottom border
    table_parts.append("└" + "┴".join("─" * (w + 2) for w in col_widths.values()) + "┘")

    return "\n".join(table_parts)


def get_csv_filename_with_prefix(base_filename: str, start_date: str, end_date: str) -> str:
    """
    Add date range prefix to CSV filename.

    Args:
        base_filename: Original filename (e.g., "usage_report.csv")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        Filename with date prefix (e.g., "2025-01-01_to_2025-01-31_usage_report.csv")
    """
    from pathlib import Path

    path = Path(base_filename)
    prefix = f"{start_date}_to_{end_date}_"
    new_name = prefix + path.name

    # Preserve directory path if present
    if path.parent != Path("."):
        return str(path.parent / new_name)
    else:
        return new_name


def write_csv(usage: dict, output_path: str, show_costs: bool = False):
    """Write per-user per-model usage data to CSV file."""
    fieldnames = [
        "user_arn",
        "username",
        "model_id",
        "model_name",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "request_count",
    ]

    if show_costs:
        fieldnames.extend([
            "input_cost",
            "output_cost",
            "cache_read_cost",
            "cache_write_cost",
            "total_cost"
        ])

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for arn in sorted(usage.keys()):
            data = usage[arn]
            username = extract_username(arn)

            # Write one row per model
            for model_id in sorted(data["models"].keys()):
                model_metrics = data["models"][model_id]
                model_name = extract_model_name(model_id)

                row = {
                    "user_arn": arn,
                    "username": username,
                    "model_id": model_id,
                    "model_name": model_name,
                    "input_tokens": model_metrics["input_tokens"],
                    "output_tokens": model_metrics["output_tokens"],
                    "cache_read_tokens": model_metrics["cache_read_tokens"],
                    "cache_write_tokens": model_metrics["cache_write_tokens"],
                    "request_count": model_metrics["request_count"],
                }

                if show_costs and "costs" in model_metrics:
                    costs = model_metrics["costs"]
                    row.update({
                        "input_cost": f"{costs['input_cost']:.4f}",
                        "output_cost": f"{costs['output_cost']:.4f}",
                        "cache_read_cost": f"{costs['cache_read_cost']:.4f}",
                        "cache_write_cost": f"{costs['cache_write_cost']:.4f}",
                        "total_cost": f"{costs['total_cost']:.4f}"
                    })

                writer.writerow(row)

            # Write summary row for this user (only if costs are shown)
            if show_costs and "costs" in data["totals"]:
                totals = data["totals"]
                total_costs = totals["costs"]

                summary_row = {
                    "user_arn": arn,
                    "username": f"TOTAL: {username}",
                    "model_id": "ALL_MODELS",
                    "model_name": "All Models",
                    "input_tokens": totals["total_input_tokens"],
                    "output_tokens": totals["total_output_tokens"],
                    "cache_read_tokens": totals["cache_read_tokens"],
                    "cache_write_tokens": totals["cache_write_tokens"],
                    "request_count": totals["request_count"],
                    "input_cost": f"{total_costs['total_input_cost']:.4f}",
                    "output_cost": f"{total_costs['total_output_cost']:.4f}",
                    "cache_read_cost": f"{total_costs['total_cache_read_cost']:.4f}",
                    "cache_write_cost": f"{total_costs['total_cache_write_cost']:.4f}",
                    "total_cost": f"{total_costs['total_cost']:.4f}"
                }

                writer.writerow(summary_row)

        # Write grand total row (only if costs are shown)
        if show_costs:
            grand_total = {
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "request_count": 0,
                "total_input_cost": 0.0,
                "total_output_cost": 0.0,
                "total_cache_read_cost": 0.0,
                "total_cache_write_cost": 0.0,
                "total_cost": 0.0
            }

            for arn in sorted(usage.keys()):
                data = usage[arn]
                if "costs" in data["totals"]:
                    totals = data["totals"]
                    total_costs = totals["costs"]
                    grand_total["total_input_tokens"] += totals["total_input_tokens"]
                    grand_total["total_output_tokens"] += totals["total_output_tokens"]
                    grand_total["cache_read_tokens"] += totals["cache_read_tokens"]
                    grand_total["cache_write_tokens"] += totals["cache_write_tokens"]
                    grand_total["request_count"] += totals["request_count"]
                    grand_total["total_input_cost"] += total_costs["total_input_cost"]
                    grand_total["total_output_cost"] += total_costs["total_output_cost"]
                    grand_total["total_cache_read_cost"] += total_costs["total_cache_read_cost"]
                    grand_total["total_cache_write_cost"] += total_costs["total_cache_write_cost"]
                    grand_total["total_cost"] += total_costs["total_cost"]

            grand_total_row = {
                "user_arn": "",
                "username": "GRAND TOTAL",
                "model_id": "ALL_USERS_ALL_MODELS",
                "model_name": "All Users, All Models",
                "input_tokens": grand_total["total_input_tokens"],
                "output_tokens": grand_total["total_output_tokens"],
                "cache_read_tokens": grand_total["cache_read_tokens"],
                "cache_write_tokens": grand_total["cache_write_tokens"],
                "request_count": grand_total["request_count"],
                "input_cost": f"{grand_total['total_input_cost']:.4f}",
                "output_cost": f"{grand_total['total_output_cost']:.4f}",
                "cache_read_cost": f"{grand_total['total_cache_read_cost']:.4f}",
                "cache_write_cost": f"{grand_total['total_cache_write_cost']:.4f}",
                "total_cost": f"{grand_total['total_cost']:.4f}"
            }

            writer.writerow(grand_total_row)

    print(f"Report written to {output_path}")


def write_csv_summary_only(usage: dict, output_path: str, show_costs: bool = False):
    """Write per-user totals (no per-model breakdown) to CSV file."""
    fieldnames = [
        "user_arn",
        "username",
        "models",
        "input_tokens",
        "output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "request_count",
    ]

    if show_costs:
        fieldnames.extend([
            "input_cost",
            "output_cost",
            "cache_read_cost",
            "cache_write_cost",
            "total_cost"
        ])

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for arn in sorted(usage.keys()):
            data = usage[arn]
            username = extract_username(arn)
            totals = data["totals"]

            # Get comma-separated list of model names
            model_names = [extract_model_name(model_id) for model_id in sorted(data["models"].keys())]
            models_display = ", ".join(model_names)

            row = {
                "user_arn": arn,
                "username": username,
                "models": models_display,
                "input_tokens": totals["total_input_tokens"],
                "output_tokens": totals["total_output_tokens"],
                "cache_read_tokens": totals["cache_read_tokens"],
                "cache_write_tokens": totals["cache_write_tokens"],
                "request_count": totals["request_count"],
            }

            if show_costs and "costs" in totals:
                total_costs = totals["costs"]
                row.update({
                    "input_cost": f"{total_costs['total_input_cost']:.4f}",
                    "output_cost": f"{total_costs['total_output_cost']:.4f}",
                    "cache_read_cost": f"{total_costs['total_cache_read_cost']:.4f}",
                    "cache_write_cost": f"{total_costs['total_cache_write_cost']:.4f}",
                    "total_cost": f"{total_costs['total_cost']:.4f}"
                })

            writer.writerow(row)

        # Write grand total row (only if costs are shown)
        if show_costs:
            grand_total = {
                "total_input_tokens": 0,
                "total_output_tokens": 0,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "request_count": 0,
                "total_input_cost": 0.0,
                "total_output_cost": 0.0,
                "total_cache_read_cost": 0.0,
                "total_cache_write_cost": 0.0,
                "total_cost": 0.0
            }

            for arn in sorted(usage.keys()):
                data = usage[arn]
                if "costs" in data["totals"]:
                    totals = data["totals"]
                    total_costs = totals["costs"]
                    grand_total["total_input_tokens"] += totals["total_input_tokens"]
                    grand_total["total_output_tokens"] += totals["total_output_tokens"]
                    grand_total["cache_read_tokens"] += totals["cache_read_tokens"]
                    grand_total["cache_write_tokens"] += totals["cache_write_tokens"]
                    grand_total["request_count"] += totals["request_count"]
                    grand_total["total_input_cost"] += total_costs["total_input_cost"]
                    grand_total["total_output_cost"] += total_costs["total_output_cost"]
                    grand_total["total_cache_read_cost"] += total_costs["total_cache_read_cost"]
                    grand_total["total_cache_write_cost"] += total_costs["total_cache_write_cost"]
                    grand_total["total_cost"] += total_costs["total_cost"]

            grand_total_row = {
                "user_arn": "",
                "username": "GRAND TOTAL",
                "models": "All Users, All Models",
                "input_tokens": grand_total["total_input_tokens"],
                "output_tokens": grand_total["total_output_tokens"],
                "cache_read_tokens": grand_total["cache_read_tokens"],
                "cache_write_tokens": grand_total["cache_write_tokens"],
                "request_count": grand_total["request_count"],
                "input_cost": f"{grand_total['total_input_cost']:.4f}",
                "output_cost": f"{grand_total['total_output_cost']:.4f}",
                "cache_read_cost": f"{grand_total['total_cache_read_cost']:.4f}",
                "cache_write_cost": f"{grand_total['total_cache_write_cost']:.4f}",
                "total_cost": f"{grand_total['total_cost']:.4f}"
            }

            writer.writerow(grand_total_row)

    print(f"Report written to {output_path}")


def main():
    args = parse_args()

    # Handle cache clearing commands
    if args.clear_full_cache:
        clear_full_cache()
        return

    if args.clear_all_cache:
        clear_all_cache()
        return

    if args.clear_pricing_cache:
        from pricing_manager import clear_pricing_cache
        clear_pricing_cache()
        return

    # Validate required arguments for normal operation
    if not args.start_date or not args.end_date:
        print("Error: --start-date and --end-date are required")
        sys.exit(1)

    # Determine sources to query
    sources = []
    if args.source == "both":
        sources = ["cloudwatch", "s3"]
    else:
        sources = [args.source]

    # Create AWS session
    session = boto3.Session(profile_name=args.profile, region_name=args.region)

    # Query each source
    all_usage = {}
    for source in sources:
        log_group_identifier = (
            args.log_group if source == "cloudwatch"
            else f"{args.s3_bucket}/{args.s3_prefix}"
        )

        # Initialize tracking variables
        missing_dates = []
        last_fetch_timestamps = {}

        # Try summary cache first (check per-day cache)
        if not args.no_cache:
            usage, missing_dates = read_summary_cache_for_range(
                source,
                log_group_identifier,
                args.start_date,
                args.end_date,
                args.region
            )

            if not missing_dates:
                # Complete cache hit
                print(f"Using cached summary data from {source} ({len(usage)} users)")
                all_usage = merge_usage(all_usage, usage)
                continue
            elif usage:
                # Partial cache hit
                cached_days = (
                    len(generate_date_list(args.start_date, args.end_date)) -
                    len(missing_dates)
                )
                print(f"Partial cache hit from {source}: {cached_days} days cached, {len(missing_dates)} days to query")
                all_usage = merge_usage(all_usage, usage)

            # Check full cache for missing dates from summary cache
            if missing_dates:
                events, missing_dates, last_fetch_timestamps = read_full_cache_for_range(
                    source,
                    log_group_identifier,
                    args.start_date,
                    args.end_date,
                    args.region
                )

                if events:
                    # Found some full cache data, aggregate it
                    cached_days = (
                        len(generate_date_list(args.start_date, args.end_date)) -
                        len(missing_dates)
                    )
                    if cached_days > 0:
                        print(f"Using cached log data from {source}: {cached_days} days cached, {len(missing_dates)} days to query")

                    day_usage = aggregate_usage(events)
                    all_usage = merge_usage(all_usage, day_usage)
        else:
            # --no-cache: query all dates
            missing_dates = generate_date_list(args.start_date, args.end_date)

        # Query AWS for missing dates
        if missing_dates:
            # Check if we have last fetch timestamps for incremental updates
            incremental_updates = {}
            for date in missing_dates:
                if date in last_fetch_timestamps:
                    incremental_updates[date] = last_fetch_timestamps[date]

            if incremental_updates:
                print(f"Incremental update for {source}: querying {len(incremental_updates)} days since last fetch")
            else:
                print(f"Querying {source} for {len(missing_dates)} missing days: {missing_dates[0]} to {missing_dates[-1]}")

            # Query only missing dates
            query_start = missing_dates[0]
            query_end = missing_dates[-1]
            start_ms = parse_date_to_epoch_ms(query_start)
            end_ms = parse_date_to_epoch_ms(query_end, end_of_day=True)

            # If we have incremental timestamps, use the earliest as start time
            if incremental_updates:
                earliest_fetch = min(incremental_updates.values())
                start_ms = earliest_fetch

            if source == "cloudwatch":
                log_results = query_logs(
                    session, args.log_group, start_ms, end_ms,
                    start_date=query_start, end_date=query_end,
                    max_workers=args.workers
                )
            elif source == "s3":
                log_results = query_s3_logs(
                    session,
                    args.s3_bucket,
                    f"{args.s3_prefix}/{args.region}",
                    query_start,
                    query_end,
                    max_workers=args.workers
                )

            if not log_results:
                print(f"No log entries found in {source} for missing dates.")
                continue

            # Split events by day
            events_by_day = split_events_by_day(log_results)

            # Determine which days need append vs overwrite
            append_mode = {}
            for date in events_by_day.keys():
                append_mode[date] = date in incremental_updates

            # Write full cache per day (with append for incremental updates)
            write_full_cache_by_day(
                source,
                log_group_identifier,
                args.region,
                events_by_day,
                append_mode
            )

            # Aggregate usage per day
            usage_by_day = {}
            for date, events in events_by_day.items():
                usage_by_day[date] = aggregate_usage(events)

            # Write summary cache per day
            write_summary_cache_by_day(
                source,
                log_group_identifier,
                args.region,
                usage_by_day
            )

            # Merge new usage into all_usage
            for day_usage in usage_by_day.values():
                all_usage = merge_usage(all_usage, day_usage)

            print(f"Cached {len(events_by_day)} days from {source}")

    # Final check
    if not all_usage:
        print("No valid Bedrock usage entries found from any source.")
        sys.exit(0)

    print(f"Total usage data for {len(all_usage)} users\n")

    # Calculate costs if requested
    if args.show_costs:
        print("Fetching pricing data...")
        from pricing_manager import get_pricing_data, calculate_costs

        try:
            pricing_data, model_id_mapping = get_pricing_data(
                session,
                pricing_region=args.pricing_region,
                force_refresh=args.refresh_pricing,
                ttl_hours=args.pricing_cache_ttl
            )

            if pricing_data:
                print(f"Loaded pricing for {len(pricing_data)} models")
                all_usage = calculate_costs(all_usage, pricing_data, model_id_mapping, args.region)
            else:
                print("Warning: Could not load pricing data. Costs will not be shown.")
                args.show_costs = False
        except Exception as e:
            print(f"Warning: Error loading pricing data: {e}")
            print("Continuing without cost calculation.")
            args.show_costs = False

    # Output based on flags
    if args.csv:
        # Write CSV with date-prefixed filename
        csv_filename = get_csv_filename_with_prefix(
            args.output,
            args.start_date,
            args.end_date
        )
        if args.summary_only:
            write_csv_summary_only(all_usage, csv_filename, show_costs=args.show_costs)
        else:
            write_csv(all_usage, csv_filename, show_costs=args.show_costs)
    else:
        # Print ASCII table to stdout
        if args.summary_only:
            table = format_ascii_table_summary_only(all_usage, show_costs=args.show_costs)
        else:
            table = format_ascii_table(all_usage, show_costs=args.show_costs)
        print(table)


if __name__ == "__main__":
    main()
