#!/usr/bin/env python3
"""
S3 log source module for querying Bedrock logs stored in S3.
Handles S3 object listing, downloading, decompression, and parsing.
Uses ThreadPoolExecutor for parallel S3 operations.
"""

import gzip
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import sys

# boto3 clients are not thread-safe, so each thread gets its own
_thread_local = threading.local()


def _get_s3_client(session):
    """Return a per-thread S3 client, creating one if needed."""
    if not hasattr(_thread_local, 's3_client'):
        _thread_local.s3_client = session.client('s3')
    return _thread_local.s3_client


def generate_date_prefixes(start_date: str, end_date: str) -> list[str]:
    """
    Generate S3 prefixes for date range (by day, not hour).

    Args:
        start_date: YYYY-MM-DD
        end_date: YYYY-MM-DD

    Returns:
        List of prefixes like ["2026/02/03/", "2026/02/04/"]
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    prefixes = []
    current = start
    while current <= end:
        # Format as YYYY/MM/DD/
        prefix = current.strftime("%Y/%m/%d/")
        prefixes.append(prefix)
        current += timedelta(days=1)

    return prefixes


def _is_log_file(key: str) -> bool:
    """Check if an S3 key is a Bedrock log file (not a marker or data file)."""
    if 'amazon-bedrock-logs-permission-check' in key:
        return False
    if '/data/' in key:
        return False
    filename = key.split('/')[-1]
    # Pattern: YYYYMMDDTHHmmssSSSSZ_<hex>.json.gz
    return filename.endswith('.json.gz') and '_' in filename and filename[0].isdigit()


def list_s3_log_files_for_date(session, bucket: str, full_prefix: str) -> list[str]:
    """
    List all log file keys under a single date prefix.

    Args:
        session: boto3 Session (thread-safe for creating clients)
        bucket: S3 bucket name
        full_prefix: Full S3 prefix including date (e.g., "AWSLogs/.../2026/02/03/")

    Returns:
        List of S3 object keys
    """
    client = _get_s3_client(session)
    paginator = client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=full_prefix)

    keys = []
    for page in page_iterator:
        if 'Contents' not in page:
            continue
        for obj in page['Contents']:
            if _is_log_file(obj['Key']):
                keys.append(obj['Key'])
    return keys


def _download_and_parse_one(session, bucket: str, key: str) -> list[dict]:
    """
    Download, decompress, and parse a single S3 log file.
    Uses thread-local S3 client for thread safety.

    Returns:
        List of CloudWatch-style event dicts
    """
    client = _get_s3_client(session)
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        compressed_data = response['Body'].read()
        decompressed_data = gzip.decompress(compressed_data)

        events = []
        for line in decompressed_data.decode('utf-8').splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            timestamp = entry.get('timestamp', 0)
            if isinstance(timestamp, str):
                try:
                    dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    timestamp = int(dt.timestamp() * 1000)
                except Exception:
                    timestamp = 0

            events.append({
                'timestamp': timestamp,
                'message': json.dumps(entry)
            })
        return events
    except Exception as e:
        print(f"  Warning: Failed to process {key}: {e}")
        return []


# Keep the old signature working for any external callers
def download_and_parse_s3_log(client, bucket: str, key: str) -> list:
    """
    Download, decompress, and parse a single S3 log file.
    Legacy wrapper — prefers _download_and_parse_one for parallel use.
    """
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        compressed_data = response['Body'].read()
        decompressed_data = gzip.decompress(compressed_data)

        log_entries = []
        for line in decompressed_data.decode('utf-8').splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                log_entry = json.loads(line)
                log_entries.append(log_entry)
            except json.JSONDecodeError:
                continue
        return log_entries
    except Exception as e:
        print(f"  Warning: Failed to process {key}: {e}")
        return []


def query_s3_logs(
    session,
    bucket: str,
    prefix: str,
    start_date: str,
    end_date: str,
    max_workers: int = 10
) -> list:
    """
    Query S3 logs within date range using parallel downloads.

    Two-phase approach:
      Phase 1 — list objects for all days in parallel
      Phase 2 — download + decompress + parse all files in parallel

    Args:
        session: boto3 Session (NOT a client — threads create their own clients)
        bucket: S3 bucket name
        prefix: Base prefix (e.g., "AWSLogs/ACCOUNT/BedrockModelInvocationLogs/REGION")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        max_workers: Thread pool size (default 10)

    Returns:
        List of log events in same format as CloudWatch logs
    """
    try:
        print(f"Querying S3 logs from s3://{bucket}/{prefix}")

        date_prefixes = generate_date_prefixes(start_date, end_date)

        # --- Phase 1: list objects for each day in parallel ---
        all_keys = []
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_date = {
                pool.submit(
                    list_s3_log_files_for_date,
                    session, bucket, f"{prefix}/{dp}"
                ): dp
                for dp in date_prefixes
            }
            for future in as_completed(future_to_date):
                dp = future_to_date[future]
                try:
                    keys = future.result()
                except Exception as exc:
                    print(f"  Warning: listing failed for {dp}: {exc}")
                    continue
                if keys:
                    print(f"  Date {dp.rstrip('/')}: found {len(keys)} log files")
                    all_keys.extend(keys)

        total_files = len(all_keys)
        if total_files == 0:
            print("Query complete. No log files found.")
            return []

        print(f"  Total files to download: {total_files}")

        # --- Phase 2: download + parse all files in parallel ---
        all_events = []
        lock = threading.Lock()
        downloaded = [0]  # mutable counter for progress

        def _download_task(key):
            events = _download_and_parse_one(session, bucket, key)
            with lock:
                all_events.extend(events)
                downloaded[0] += 1
                count = downloaded[0]
            # Progress every 50 files
            if count % 50 == 0:
                print(f"  Downloaded {count}/{total_files} files...")
            return len(events)

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = [pool.submit(_download_task, key) for key in all_keys]
            # Wait for all to complete; exceptions are captured inside _download_task
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    print(f"  Warning: download task failed: {exc}")

        print(f"Query complete. Processed {total_files} files, found {len(all_events)} log entries.")
        return all_events

    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code == "AccessDenied" or error_code == "NoSuchBucket":
            print(f"\nERROR: S3 access error ({error_code}). Your IAM user/role needs:")
            print("  - s3:ListBucket on the bucket")
            print("  - s3:GetObject on objects under the prefix")
            print(f"  Bucket: {bucket}")
            print(f"  Prefix: {prefix}")
            print("\nTry using a different --profile with appropriate permissions.")
            sys.exit(1)
        raise
