# AWS Bedrock Usage

CLI tools for AWS Bedrock usage reporting and log analysis.

- **Usage reports** (`bedrock_usage_report.py`): Per-user usage reports with per-model token breakdown and estimated costs. Each row shows a user-model combination with input/output/cache tokens and request counts.
- **Full log download** (`bedrock_download_logs.py`): Download complete invocation logs (prompts, responses, all metadata) as JSONL with user filtering and field selection.

## Features

- **Dual log sources**: Query logs from S3 (default), CloudWatch, or both
- **Parallel downloads**: Threaded S3 and CloudWatch queries for ~10x faster data fetching
- **Per-model breakdown**: Token usage split by model for each IAM user
- **Cost estimation**: Automatic cost calculation via AWS Pricing API with local caching
- **Flexible output**: Formatted ASCII table (default) or CSV export
- **Smart caching**: Two-layer per-day cache (summary + full) for fast repeat queries and incremental date range expansion
- **Cache token tracking**: Tracks cache read/write tokens separately for prompt caching visibility
- **Full log download**: Export complete invocation logs as JSONL with user filtering and field selection

## Prerequisites

- Python 3.13+
- AWS CLI profile with appropriate permissions configured
- `boto3` library

### IAM Permissions

**S3 logs** (default source): `s3:ListBucket`, `s3:GetObject`

**CloudWatch logs**: `logs:FilterLogEvents`

**Cost estimation**: `pricing:GetProducts`

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install boto3
```

## Usage

### Basic report (ASCII table from S3 logs)

```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile>
```

### CSV export

```bash
# Default filename: 2025-01-01_to_2025-01-31_usage_report.csv
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --csv

# Custom filename (date range prefix is added automatically)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --csv --output my_report.csv
```

### CloudWatch or merged sources

```bash
# CloudWatch only
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source cloudwatch \
  --profile <aws-profile>

# Both S3 and CloudWatch (merged)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source both \
  --profile <aws-profile>
```

### Cost estimation

Costs are calculated and shown by default using AWS Pricing API data.

```bash
# Disable costs
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --no-costs

# Force refresh pricing data
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --refresh-pricing

# Custom pricing cache TTL (hours)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --pricing-cache-ttl 48
```

Pricing sources: AWS Pricing API (primary, cached for 24h) with [LiteLLM](https://github.com/BerriAI/litellm/blob/main/model_prices_and_context_window.json) fallback for newer models.

> Costs are estimates based on list prices. Actual billing may differ due to discounts, credits, or reserved capacity.

### Parallel downloads

Both S3 and CloudWatch queries run in parallel using a thread pool (default: 10 workers). S3 parallelizes listing and downloading; CloudWatch parallelizes by querying each day independently. Control with `--workers`:

```bash
# Faster downloads with more threads
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --workers 20

# Sequential mode (useful for debugging)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --workers 1
```

### Cache management

The tool caches data per-day, so overlapping date ranges automatically reuse cached days.

```bash
# Bypass cache (fresh query, still writes cache)
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --no-cache

# Clear full cache (keeps summary cache)
python bedrock_usage_report.py --clear-full-cache

# Clear all cache
python bedrock_usage_report.py --clear-all-cache

# Clear pricing cache
python bedrock_usage_report.py --clear-pricing-cache
```

## Downloading full logs

`bedrock_download_logs.py` downloads complete Bedrock invocation logs as JSONL. It shares the same cache as the usage report, so previously fetched days are reused automatically.

### Basic download (JSONL to stdout)

```bash
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile>
```

### Filter by users

```bash
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --users alice,bob
```

### Save to file

```bash
# Creates: 2025-01-01_to_2025-01-31_logs.jsonl
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --output logs.jsonl

# With user filter: 2025-01-01_to_2025-01-31_alice_bob_logs.jsonl
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --users alice,bob \
  --output logs.jsonl
```

### Field selection

```bash
# Include only specific fields
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --fields timestamp,modelId,identity,input

# Exclude fields
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --exclude-fields schemaType,schemaVersion
```

Each JSONL line contains the original AWS log structure enriched with `identity.username` and `modelName`. All status messages go to stderr, so piping works cleanly (e.g., `| jq .`, `| wc -l`).

## How caching works

Each day is cached independently:

```
.cache/
├── full/           # Complete log events (for reprocessing)
├── summary/        # Pre-aggregated per-user per-model stats (for fast reports)
└── pricing/        # AWS Pricing API responses
```

- Querying Jan 1-31 creates 31 daily cache files
- Later querying Jan 15 - Feb 5 reuses Jan 15-31 from cache, only fetches Feb 1-5
- Today's data is always refreshed incrementally

## Project structure

```
bedrock_usage_report.py   # Usage report CLI (ASCII table / CSV)
bedrock_download_logs.py  # Full log download CLI (JSONL)
cache_manager.py          # Two-layer per-day caching (full + summary)
pricing_manager.py        # AWS Pricing API queries, caching, and cost calculation
s3_log_source.py          # S3 log fetching, decompression, and parsing
```
