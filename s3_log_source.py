#!/usr/bin/env python3
"""
S3 log source module for querying Bedrock logs stored in S3.
Handles S3 object listing, downloading, decompression, and parsing.
"""

import gzip
import json
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import sys


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


def download_and_parse_s3_log(client, bucket: str, key: str) -> list:
    """
    Download, decompress, and parse a single S3 log file.

    Args:
        client: boto3 S3 client
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        List of parsed log entries
    """
    try:
        # GetObject from S3
        response = client.get_object(Bucket=bucket, Key=key)
        compressed_data = response['Body'].read()

        # Decompress with gzip
        decompressed_data = gzip.decompress(compressed_data)

        # Parse JSON Lines (one JSON object per line)
        log_entries = []
        for line in decompressed_data.decode('utf-8').splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                log_entry = json.loads(line)
                log_entries.append(log_entry)
            except json.JSONDecodeError:
                # Skip malformed lines
                continue

        return log_entries
    except Exception as e:
        print(f"  Warning: Failed to process {key}: {e}")
        return []


def query_s3_logs(
    client,
    bucket: str,
    prefix: str,
    start_date: str,
    end_date: str
) -> list:
    """
    Query S3 logs within date range using path prefixes.

    Args:
        client: boto3 S3 client
        bucket: S3 bucket name
        prefix: Base prefix (e.g., "AWSLogs/ACCOUNT/BedrockModelInvocationLogs/REGION")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of log events in same format as CloudWatch logs
    """
    try:
        print(f"Querying S3 logs from s3://{bucket}/{prefix}")

        # Generate date prefixes to query
        date_prefixes = generate_date_prefixes(start_date, end_date)

        all_events = []
        total_files = 0

        for date_prefix in date_prefixes:
            # Full prefix for this date
            full_prefix = f"{prefix}/{date_prefix}"

            # List objects under this date prefix
            paginator = client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=full_prefix
            )

            log_files = []
            for page in page_iterator:
                if 'Contents' not in page:
                    continue

                for obj in page['Contents']:
                    key = obj['Key']

                    # Skip permission check marker files
                    if 'amazon-bedrock-logs-permission-check' in key:
                        continue

                    # Skip data/ subdirectory (contains input files only)
                    if '/data/' in key:
                        continue

                    # Only process main log files matching pattern
                    # Pattern: YYYYMMDDTHHmmssSSSSZ_<hex>.json.gz
                    filename = key.split('/')[-1]
                    if filename.endswith('.json.gz') and '_' in filename:
                        # Basic check: starts with digit and has timestamp format
                        if filename[0].isdigit():
                            log_files.append(key)

            if log_files:
                print(f"  Date {date_prefix.rstrip('/')}: found {len(log_files)} log files")
                total_files += len(log_files)

                # Download and parse each log file
                for key in log_files:
                    log_entries = download_and_parse_s3_log(client, bucket, key)

                    # Convert to CloudWatch-style events format
                    for entry in log_entries:
                        # Extract timestamp if available, otherwise use 0
                        timestamp = entry.get('timestamp', 0)
                        if isinstance(timestamp, str):
                            # Parse ISO timestamp if needed
                            try:
                                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                                timestamp = int(dt.timestamp() * 1000)
                            except:
                                timestamp = 0

                        # Create CloudWatch-style event
                        event = {
                            'timestamp': timestamp,
                            'message': json.dumps(entry)
                        }
                        all_events.append(event)

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
