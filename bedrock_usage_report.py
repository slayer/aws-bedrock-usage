#!/usr/bin/env python3
"""
CLI tool to query AWS CloudWatch logs from BedrockLogging and generate
a CSV report of Bedrock usage per user (identified by IAM ARN).
"""

import argparse
import csv
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

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
        default="BedrockLogging6",
        help="CloudWatch log group name (default: BedrockLogging6)",
    )
    parser.add_argument(
        "--s3-bucket",
        default="aytm-bedrock-logs",
        help="S3 bucket name for Bedrock logs (default: aytm-bedrock-logs)",
    )
    parser.add_argument(
        "--s3-prefix",
        default="AWSLogs/023788696405/BedrockModelInvocationLogs",
        help="S3 prefix for Bedrock logs (default: AWSLogs/023788696405/BedrockModelInvocationLogs)",
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
        "--output",
        default="usage_report.csv",
        help="Output CSV file path (default: usage_report.csv)",
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


def query_logs(client, log_group: str, start_ms: int, end_ms: int) -> list:
    """Query CloudWatch Logs using filter_log_events with pagination."""
    from botocore.exceptions import ClientError

    print(f"Querying CloudWatch Logs from {log_group}...")

    all_events = []
    next_token = None
    page_count = 0

    try:
        while True:
            page_count += 1
            kwargs = {
                "logGroupName": log_group,
                "startTime": start_ms,
                "endTime": end_ms,
                "filterPattern": "inputTokenCount",  # Filter for Bedrock logs
            }
            if next_token:
                kwargs["nextToken"] = next_token

            response = client.filter_log_events(**kwargs)
            events = response.get("events", [])
            all_events.extend(events)

            print(f"  Page {page_count}: retrieved {len(events)} events (total: {len(all_events)})")

            next_token = response.get("nextToken")
            if not next_token:
                break
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
    """Aggregate usage statistics by user ARN."""
    # Structure: {arn: {metrics}}
    usage = defaultdict(
        lambda: {
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "cache_read_tokens": 0,
            "cache_write_tokens": 0,
            "request_count": 0,
            "models_used": set(),
        }
    )

    for event in log_results:
        # filter_log_events returns events with 'message' field directly
        message = event.get("message")
        if not message:
            continue

        entry = process_log_entry(message)
        if not entry:
            continue

        arn = entry["arn"]
        usage[arn]["total_input_tokens"] += entry["input_tokens"]
        usage[arn]["total_output_tokens"] += entry["output_tokens"]
        usage[arn]["cache_read_tokens"] += entry["cache_read_tokens"]
        usage[arn]["cache_write_tokens"] += entry["cache_write_tokens"]
        usage[arn]["request_count"] += 1
        usage[arn]["models_used"].add(entry["model_id"])

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
    result = dict(usage1)  # Copy

    for arn, metrics in usage2.items():
        if arn in result:
            # Sum tokens
            result[arn]["total_input_tokens"] += metrics["total_input_tokens"]
            result[arn]["total_output_tokens"] += metrics["total_output_tokens"]
            result[arn]["cache_read_tokens"] += metrics["cache_read_tokens"]
            result[arn]["cache_write_tokens"] += metrics["cache_write_tokens"]
            result[arn]["request_count"] += metrics["request_count"]
            # Union models
            result[arn]["models_used"] = result[arn]["models_used"].union(metrics["models_used"])
        else:
            result[arn] = metrics.copy()
            result[arn]["models_used"] = set(metrics["models_used"])

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


def write_csv(usage: dict, output_path: str):
    """Write aggregated usage data to CSV file."""
    fieldnames = [
        "user_arn",
        "username",
        "total_input_tokens",
        "total_output_tokens",
        "cache_read_tokens",
        "cache_write_tokens",
        "request_count",
        "models_used",
    ]

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for arn, metrics in sorted(usage.items()):
            writer.writerow(
                {
                    "user_arn": arn,
                    "username": extract_username(arn),
                    "total_input_tokens": metrics["total_input_tokens"],
                    "total_output_tokens": metrics["total_output_tokens"],
                    "cache_read_tokens": metrics["cache_read_tokens"],
                    "cache_write_tokens": metrics["cache_write_tokens"],
                    "request_count": metrics["request_count"],
                    "models_used": ",".join(sorted(metrics["models_used"])),
                }
            )

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
                client = session.client("logs")
                log_results = query_logs(client, args.log_group, start_ms, end_ms)
            elif source == "s3":
                client = session.client("s3")
                log_results = query_s3_logs(
                    client,
                    args.s3_bucket,
                    f"{args.s3_prefix}/{args.region}",
                    query_start,
                    query_end
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

    print(f"Total usage data for {len(all_usage)} users")

    # Write CSV
    write_csv(all_usage, args.output)


if __name__ == "__main__":
    main()
