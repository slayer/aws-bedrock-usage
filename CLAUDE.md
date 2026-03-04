# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CLI tool suite for AWS Bedrock usage reporting and log analysis:
- **bedrock_usage_report.py** — Generates CSV/ASCII usage reports per IAM user with per-model breakdown for cost calculation.
- **bedrock_download_logs.py** — Downloads full invocation logs (prompts, responses, metadata) as JSONL with user filtering.

## Setup

```bash
source venv/bin/activate
```

## Usage

**Quick start**: Activate virtual environment and run the script:
```bash
source venv/bin/activate && python bedrock_usage_report.py --start-date 2025-01-01 --end-date 2025-01-31 --profile <aws-profile>
```

**Default behavior**: Outputs a formatted ASCII table to stdout.

**CSV export**: Add the `--csv` flag to write a CSV file (filename will be prefixed with the date range).

### Basic usage - ASCII table output (S3 - default)
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile>
```

### CSV export with default filename
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --csv
# Creates: 2025-01-01_to_2025-01-31_usage_report.csv
```

### CSV export with custom filename
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --csv \
  --output my_report.csv
# Creates: 2025-01-01_to_2025-01-31_my_report.csv
```

### CloudWatch logs
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source cloudwatch \
  --profile <aws-profile>
```

### Query both sources and merge
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source both \
  --profile <aws-profile>
```

### Custom S3 bucket/prefix
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --source s3 \
  --s3-bucket my-bucket \
  --s3-prefix AWSLogs/123456789012/BedrockModelInvocationLogs \
  --profile <aws-profile>
```

### Cost Calculation

**Costs are shown by default** - estimated costs are calculated and displayed based on AWS Pricing API:

```bash
# Default behavior - costs are shown
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile>

# Disable costs if not needed
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --no-costs
```

**Pricing Cache:**
- Pricing data is cached locally in `.cache/pricing/bedrock_pricing.json`
- Default TTL: 24 hours
- Force refresh: `--refresh-pricing`
- Clear cache: `--clear-pricing-cache`
- Custom TTL: `--pricing-cache-ttl 48` (hours)

**Pricing Region:**
AWS Pricing API is only available in certain regions. Use `--pricing-region` to specify:
- `us-east-1` (default)
- `eu-central-1`
- `ap-south-1`

**Pricing Sources:**
- **Primary**: AWS Pricing API (queried and cached)
- **Fallback**: [LiteLLM model pricing database](https://github.com/BerriAI/litellm/blob/main/model_prices_and_context_window.json) for models not yet in AWS API

Models with fallback pricing include: Claude Opus 4.5, Claude Haiku 4.5, Claude 3.5 Haiku, and other models. The LiteLLM database is community-maintained and reflects actual AWS Bedrock pricing.

**Note:** Costs are estimates based on AWS list prices and may not reflect your actual AWS bill (which may include discounts, credits, reserved capacity, etc.).

### Parallel downloads

Both S3 and CloudWatch queries run in parallel threads (default: 10 workers). S3 parallelizes listing + downloading; CloudWatch queries each day independently. Use `--workers` to tune:
```bash
python bedrock_usage_report.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --workers 20
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

# Clear pricing cache
python bedrock_usage_report.py --clear-pricing-cache
```

The tool uses a two-layer per-day caching system:
- **Summary cache**: Fast report generation from pre-aggregated per-model statistics (instant)
- **Full cache**: Complete log events for reprocessing without AWS API calls

**Per-model statistics**: Both cache layers store usage data broken down by user AND model, allowing accurate cost calculations since Bedrock pricing varies by model.

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
│       ├── 2025-01-01.json  # Raw log events
│       ├── 2025-01-02.json
│       └── ...
└── summary/
    └── {source}/{region}/{log_group_hash}/
        ├── 2025-01-01.json  # Per-user per-model aggregated stats
        └── ...
```

**Summary cache format:**
```json
{
  "usage": {
    "arn:aws:iam::123:user/alice": {
      "models": {
        "model-id-1": {
          "input_tokens": 100,
          "output_tokens": 200,
          "cache_read_tokens": 50,
          "cache_write_tokens": 25,
          "request_count": 5
        }
      },
      "totals": {
        "total_input_tokens": 100,
        "total_output_tokens": 200,
        "cache_read_tokens": 50,
        "cache_write_tokens": 25,
        "request_count": 5,
        "models_used": ["model-id-1"]
      }
    }
  }
}
```

## Downloading Full Logs (bedrock_download_logs.py)

Downloads complete Bedrock invocation logs (prompts, responses, all metadata) as JSONL. Shares the same cache infrastructure as the usage report script.

### Basic usage - JSONL to stdout
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

### Save to file (auto-prefixed with date range)
```bash
python bedrock_download_logs.py \
  --start-date 2025-01-01 \
  --end-date 2025-01-31 \
  --profile <aws-profile> \
  --output logs.jsonl
# Creates: 2025-01-01_to_2025-01-31_logs.jsonl
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

### JSONL output format
Each line is a JSON object with the original AWS log structure plus enrichments (`identity.username`, `modelName`):
```json
{
  "timestamp": "2026-02-10T00:00:00Z",
  "modelId": "arn:aws:bedrock:...",
  "modelName": "Claude Sonnet 4.5",
  "identity": {"arn": "arn:aws:iam::123:user/alice", "username": "alice"},
  "input": {"inputTokenCount": 18034, "inputBodyJson": {...}},
  "output": {"outputTokenCount": 667, "outputBodyJson": {...}}
}
```

All progress/status messages go to stderr; JSONL output goes to stdout, so piping works cleanly.

## AWS Requirements

### For CloudWatch Logs
The IAM user/role needs `logs:FilterLogEvents` permission on the target log group (default: `BedrockModelInvocationLogging`).

### For S3 Logs (default)
The IAM user/role needs:
- `s3:ListBucket` on the bucket
- `s3:GetObject` on objects under the prefix

Default S3 location: `s3://<your-bucket>/AWSLogs/<account-id>/BedrockModelInvocationLogs/<region>/`

### For Pricing API (optional, for --show-costs)
The IAM user/role needs `pricing:GetProducts` permission.
