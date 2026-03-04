#!/usr/bin/env python3
"""
CLI tool to download full Bedrock invocation logs (prompts, responses, metadata)
as JSONL. Reuses the same cache and AWS query infrastructure as bedrock_usage_report.py.
"""

import argparse
import json
import sys
from datetime import datetime, timezone

import boto3

from bedrock_usage_report import (
    parse_date_to_epoch_ms,
    extract_username,
    extract_model_name,
    split_events_by_day,
    query_logs,
)
from cache_manager import (
    read_full_cache_for_range,
    write_full_cache_by_day,
    clear_full_cache,
    clear_all_cache,
    generate_date_list,
)
from s3_log_source import query_s3_logs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download full Bedrock invocation logs as JSONL"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date in YYYY-MM-DD format",
    )
    parser.add_argument(
        "--source",
        default="s3",
        choices=["s3", "cloudwatch", "both"],
        help="Log source: s3 (default), cloudwatch, or both",
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
        help="S3 prefix for Bedrock logs",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel threads (default: 10)",
    )
    parser.add_argument(
        "--users",
        default=None,
        help="Comma-separated usernames to filter (e.g., alice,bob)",
    )

    # Field selection (mutually exclusive)
    field_group = parser.add_mutually_exclusive_group()
    field_group.add_argument(
        "--fields",
        default=None,
        help="Comma-separated top-level fields to include (e.g., timestamp,modelId,identity,input)",
    )
    field_group.add_argument(
        "--exclude-fields",
        default=None,
        help="Comma-separated top-level fields to exclude (e.g., schemaType,schemaVersion)",
    )

    parser.add_argument(
        "--output", "-o",
        default=None,
        help="Output file path (.jsonl), auto-prefixed with date range. Omit for stdout.",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="Skip cache read (still writes cache)",
    )
    parser.add_argument(
        "--clear-full-cache",
        action="store_true",
        help="Clear full cache and exit",
    )
    parser.add_argument(
        "--clear-all-cache",
        action="store_true",
        help="Clear all cache and exit",
    )
    return parser.parse_args()


def log(msg):
    """Print status messages to stderr so JSONL output stays clean on stdout."""
    print(msg, file=sys.stderr)


def matches_user_filter(arn, user_filter):
    """Check if an ARN matches any username in the filter set."""
    if not user_filter:
        return True
    username = extract_username(arn)
    return username.lower() in user_filter


def enrich_entry(entry):
    """Add computed fields: identity.username and modelName."""
    identity = entry.get("identity", {})
    arn = identity.get("arn", "")
    if arn:
        identity["username"] = extract_username(arn)
        entry["identity"] = identity

    model_id = entry.get("modelId", "")
    if model_id:
        entry["modelName"] = extract_model_name(model_id)

    return entry


def filter_fields(entry, include=None, exclude=None):
    """Apply field inclusion/exclusion to a log entry."""
    if include:
        return {k: v for k, v in entry.items() if k in include}
    if exclude:
        return {k: v for k, v in entry.items() if k not in exclude}
    return entry


def process_event(event, user_filter=None, include_fields=None, exclude_fields=None):
    """
    Parse a cached event, filter, enrich, and return as dict or None.
    Events are stored as {"timestamp": ..., "message": "<json-string>"}.
    """
    message = event.get("message", "")
    try:
        entry = json.loads(message)
    except json.JSONDecodeError:
        return None

    # Filter by user
    identity = entry.get("identity", {})
    arn = identity.get("arn", "")
    if not matches_user_filter(arn, user_filter):
        return None

    enrich_entry(entry)
    return filter_fields(entry, include=include_fields, exclude=exclude_fields)


def fetch_events_for_source(source, args, session):
    """
    Fetch events for a single source (s3 or cloudwatch), using cache.
    Returns events as a list, writing cache for newly fetched days.
    """
    if source == "s3":
        log_group_id = f"{args.s3_bucket}/{args.s3_prefix}"
    else:
        log_group_id = args.log_group

    start_ms = parse_date_to_epoch_ms(args.start_date)
    end_ms = parse_date_to_epoch_ms(args.end_date, end_of_day=True)

    # Check cache
    if not args.no_cache:
        cached_events, missing_dates, last_fetch_ts = read_full_cache_for_range(
            source, log_group_id, args.start_date, args.end_date, args.region
        )
    else:
        cached_events = []
        missing_dates = generate_date_list(args.start_date, args.end_date)
        last_fetch_ts = {}

    if not missing_dates:
        log(f"  [{source}] All days cached ({len(cached_events)} events)")
        return cached_events

    log(f"  [{source}] {len(missing_dates)} day(s) to fetch from AWS")

    # Determine effective date range for missing days
    missing_start = min(missing_dates)
    missing_end = max(missing_dates)
    query_start_ms = parse_date_to_epoch_ms(missing_start)
    query_end_ms = parse_date_to_epoch_ms(missing_end, end_of_day=True)

    # For today's incremental update, shift start to last fetch timestamp
    for date, ts in last_fetch_ts.items():
        if date == missing_start:
            query_start_ms = max(query_start_ms, ts)

    # Query AWS
    if source == "s3":
        new_events = query_s3_logs(
            session, args.s3_bucket,
            f"{args.s3_prefix}/{args.region}",
            missing_start, missing_end,
            max_workers=args.workers,
        )
    else:
        new_events = query_logs(
            session, args.log_group,
            query_start_ms, query_end_ms,
            start_date=missing_start, end_date=missing_end,
            max_workers=args.workers,
        )

    # Write new events to cache, split by day
    if new_events:
        events_by_day = split_events_by_day(new_events)
        append_mode = {d: d in last_fetch_ts for d in events_by_day}
        write_full_cache_by_day(source, log_group_id, args.region, events_by_day, append_mode)

    all_events = cached_events + new_events
    log(f"  [{source}] Total: {len(all_events)} events")
    return all_events


def build_output_path(args):
    """Build date-prefixed output filename."""
    base = args.output
    prefix = f"{args.start_date}_to_{args.end_date}"

    # Include user names in filename when filtering
    if args.users:
        user_list = sorted(args.users.split(","))
        user_part = "_".join(u.strip() for u in user_list)
        return f"{prefix}_{user_part}_{base}"

    return f"{prefix}_{base}"


def main():
    args = parse_args()

    # Handle cache clearing
    if args.clear_full_cache:
        clear_full_cache()
        return
    if args.clear_all_cache:
        clear_all_cache()
        return

    # Parse filter options
    user_filter = None
    if args.users:
        user_filter = {u.strip().lower() for u in args.users.split(",")}

    include_fields = None
    exclude_fields = None
    if args.fields:
        include_fields = {f.strip() for f in args.fields.split(",")}
    elif args.exclude_fields:
        exclude_fields = {f.strip() for f in args.exclude_fields.split(",")}

    # Set up AWS session
    session_kwargs = {}
    if args.profile:
        session_kwargs["profile_name"] = args.profile
    if args.region:
        session_kwargs["region_name"] = args.region
    session = boto3.Session(**session_kwargs)

    log(f"Downloading logs from {args.start_date} to {args.end_date}")

    # Fetch events from requested sources
    sources = [args.source] if args.source != "both" else ["s3", "cloudwatch"]
    all_events = []
    for source in sources:
        events = fetch_events_for_source(source, args, session)
        all_events.extend(events)

    if not all_events:
        log("No log events found.")
        return

    # Dedup by requestId when using both sources
    if args.source == "both":
        seen = set()
        deduped = []
        for event in all_events:
            try:
                data = json.loads(event.get("message", ""))
                req_id = data.get("requestId")
            except (json.JSONDecodeError, AttributeError):
                req_id = None
            if req_id and req_id in seen:
                continue
            if req_id:
                seen.add(req_id)
            deduped.append(event)
        log(f"Deduplication: {len(all_events)} -> {len(deduped)} events")
        all_events = deduped

    # Open output
    output_path = None
    if args.output:
        output_path = build_output_path(args)
        out = open(output_path, "w")
    else:
        out = sys.stdout

    # Process and write JSONL
    written = 0
    try:
        for event in all_events:
            result = process_event(event, user_filter, include_fields, exclude_fields)
            if result is not None:
                out.write(json.dumps(result, default=str) + "\n")
                written += 1
    finally:
        if output_path:
            out.close()

    if output_path:
        log(f"Wrote {written} entries to {output_path}")
    else:
        log(f"Wrote {written} entries to stdout")


if __name__ == "__main__":
    main()
