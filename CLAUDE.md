# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CLI tool that queries AWS CloudWatch logs from Bedrock logging and generates CSV usage reports per IAM user.

## Setup

```bash
source venv/bin/activate
```

## Usage

### Basic usage (S3 - default)
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile aytm \
  --output usage_report.csv
```

### CloudWatch logs
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source cloudwatch \
  --profile <aws-profile> \
  --output usage_report.csv
```

### Query both sources and merge
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source both \
  --profile aytm \
  --output usage_report.csv
```

### Custom S3 bucket/prefix
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source s3 \
  --s3-bucket my-bucket \
  --s3-prefix AWSLogs/123456789012/BedrockModelInvocationLogs \
  --profile aytm
```

### Cache management
```bash
# Force fresh query and update cache (bypasses cache read, but still writes cache)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --no-cache

# Clear full cache to save disk space (keeps summary cache for fast reports)
python bedrock_usage_report.py --clear-full-cache

# Clear all cache (full and summary)
python bedrock_usage_report.py --clear-all-cache
```

The tool uses a two-layer per-day caching system:
- **Summary cache**: Fast CSV generation from pre-aggregated data (instant)
- **Full cache**: Complete log events for reprocessing without AWS API calls

**Per-day granularity**: Cache is organized by individual days, enabling efficient reuse across different date ranges.

#### How Per-Day Caching Works

Each day is cached independently in its own file. This means:
- Querying Jan 1-31 creates 31 cache files (one per day)
- Later querying Jan 25-31 reuses those 7 cached days instantly
- Querying Jan 15 - Feb 5 reuses cached Jan 15-31 and only queries AWS for Feb 1-5

**Benefits:**
- **No duplicate queries**: Overlapping date ranges automatically reuse cached days
- **Incremental updates**: Add new days without re-downloading old days
- **Flexible querying**: Query any date range and reuse all available cached data

**Today's data**: The current day is always treated as incomplete and will be incrementally updated on each query to capture the latest logs without re-downloading the entire day's data.

**Cache structure:**
```
.cache/
├── full/
│   └── {source}/{region}/{log_group_hash}/
│       ├── 2025-01-01.json
│       ├── 2025-01-02.json
│       └── ...
└── summary/
    └── {source}/{region}/{log_group_hash}/
        ├── 2025-01-01.json
        └── ...
```

## AWS Requirements

### For CloudWatch Logs
The IAM user/role needs `logs:FilterLogEvents` permission on the target log group (default: `BedrockLogging6`).

### For S3 Logs (default)
The IAM user/role needs:
- `s3:ListBucket` on the bucket
- `s3:GetObject` on objects under the prefix

Default S3 location: `s3://aytm-bedrock-logs/AWSLogs/023788696405/BedrockModelInvocationLogs/us-east-1/`
