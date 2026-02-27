#!/usr/bin/env python3
"""
Pricing manager for AWS Bedrock cost calculation.
Fetches pricing data from AWS Pricing API and caches it locally.
"""

import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional
from collections import defaultdict


# Fallback pricing (per 1K tokens) for models not yet in AWS Pricing API
# Source: LiteLLM model_prices_and_context_window.json
# https://github.com/BerriAI/litellm/blob/main/model_prices_and_context_window.json
# This is maintained by the community and reflects actual AWS Bedrock pricing
FALLBACK_PRICING = {
    "Claude Opus 4.5": {
        "input_tokens_per_1k": 0.005,      # 5e-06 per token = $5/MTok
        "output_tokens_per_1k": 0.025,     # 2.5e-05 per token = $25/MTok
        "cache_write_tokens_per_1k": 0.00625,  # 6.25e-06 per token
        "cache_read_tokens_per_1k": 0.0005,    # 5e-07 per token
    },
    "Claude Sonnet 4.5": {
        "input_tokens_per_1k": 0.003,      # 3e-06 per token = $3/MTok
        "output_tokens_per_1k": 0.015,     # 1.5e-05 per token = $15/MTok
        "cache_write_tokens_per_1k": 0.00375,  # 3.75e-06 per token
        "cache_read_tokens_per_1k": 0.0003,    # 3e-07 per token
    },
    "Claude Sonnet 4": {
        "input_tokens_per_1k": 0.003,      # 3e-06 per token
        "output_tokens_per_1k": 0.015,     # 1.5e-05 per token
        "cache_write_tokens_per_1k": 0.00375,
        "cache_read_tokens_per_1k": 0.0003,
    },
    "Claude Haiku 4.5": {
        "input_tokens_per_1k": 0.001,      # 1e-06 per token = $1/MTok
        "output_tokens_per_1k": 0.005,     # 5e-06 per token = $5/MTok
        "cache_write_tokens_per_1k": 0.00125,  # 1.25e-06 per token
        "cache_read_tokens_per_1k": 0.0001,    # 1e-07 per token
    },
    "Claude 3.5 Haiku": {
        "input_tokens_per_1k": 0.0008,     # 8e-07 per token (from LiteLLM)
        "output_tokens_per_1k": 0.004,     # 4e-06 per token
        "cache_write_tokens_per_1k": 0.001,    # 1e-06 per token
        "cache_read_tokens_per_1k": 0.00008,   # 8e-08 per token
    },
    "Claude 3.5 Sonnet": {
        "input_tokens_per_1k": 0.003,      # 3e-06 per token
        "output_tokens_per_1k": 0.015,     # 1.5e-05 per token
        "cache_write_tokens_per_1k": 0.00375,  # 3.75e-06 per token
        "cache_read_tokens_per_1k": 0.0003,    # 3e-07 per token
    },
    "Claude Opus 4": {
        "input_tokens_per_1k": 0.015,      # 1.5e-05 per token = $15/MTok
        "output_tokens_per_1k": 0.075,     # 7.5e-05 per token = $75/MTok
        "cache_write_tokens_per_1k": 0.01875,  # 1.875e-05 per token
        "cache_read_tokens_per_1k": 0.0015,    # 1.5e-06 per token
    },
    "Claude 3 Opus": {
        "input_tokens_per_1k": 0.015,      # Legacy pricing
        "output_tokens_per_1k": 0.075,
        "cache_write_tokens_per_1k": 0.0,  # No cache support
        "cache_read_tokens_per_1k": 0.0,
    },
    "Claude 3 Sonnet": {
        "input_tokens_per_1k": 0.003,
        "output_tokens_per_1k": 0.015,
        "cache_write_tokens_per_1k": 0.0,  # No cache support
        "cache_read_tokens_per_1k": 0.0,
    },
    "Claude 3 Haiku": {
        "input_tokens_per_1k": 0.00025,    # 2.5e-07 per token
        "output_tokens_per_1k": 0.00125,   # 1.25e-06 per token
        "cache_write_tokens_per_1k": 0.0,  # No cache support
        "cache_read_tokens_per_1k": 0.0,
    },
}


# Cache directory paths
CACHE_DIR = Path(".cache")
PRICING_CACHE_DIR = CACHE_DIR / "pricing"
PRICING_CACHE_FILE = PRICING_CACHE_DIR / "bedrock_pricing.json"


def ensure_pricing_cache_dir():
    """Create pricing cache directory if it doesn't exist."""
    PRICING_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def get_pricing_cache_path() -> Path:
    """Get path to pricing cache file."""
    return PRICING_CACHE_FILE


def read_pricing_cache(ttl_hours: int = 24) -> Optional[dict]:
    """
    Read pricing cache if it exists and is not stale.

    Args:
        ttl_hours: Cache TTL in hours (default: 24)

    Returns:
        Cached pricing data dict or None if cache miss/stale
    """
    cache_file = get_pricing_cache_path()

    if not cache_file.exists():
        return None

    try:
        with open(cache_file, "r") as f:
            data = json.load(f)

        # Check for required keys (backward compatibility)
        if "metadata" not in data or "pricing" not in data:
            print("Warning: Old pricing cache format detected, will refresh")
            return None

        # Check cache age
        metadata = data["metadata"]
        cached_at_str = metadata.get("cached_at")
        if not cached_at_str:
            return None

        cached_at = datetime.fromisoformat(cached_at_str.replace("Z", "+00:00"))
        age_hours = (datetime.now(timezone.utc) - cached_at).total_seconds() / 3600

        if age_hours > ttl_hours:
            print(f"Pricing cache is {age_hours:.1f} hours old (TTL: {ttl_hours}h), will refresh")
            return None

        print(f"Using pricing cache from {cached_at_str} ({age_hours:.1f}h old)")
        return data

    except (json.JSONDecodeError, IOError, ValueError, KeyError) as e:
        print(f"Warning: Error reading pricing cache: {e}")
        return None


def write_pricing_cache(pricing_data: dict, model_id_mapping: dict, pricing_region: str):
    """
    Write pricing data to cache.

    Args:
        pricing_data: Dict mapping model names to pricing info
        model_id_mapping: Dict mapping model IDs to model names
        pricing_region: AWS region used for pricing query
    """
    ensure_pricing_cache_dir()
    cache_file = get_pricing_cache_path()

    cache_data = {
        "metadata": {
            "cached_at": datetime.now(timezone.utc).isoformat(),
            "pricing_region": pricing_region,
            "service_code": "AmazonBedrockService",
            "model_count": len(pricing_data),
            "version": "1.0"
        },
        "pricing": pricing_data,
        "model_id_mapping": model_id_mapping
    }

    with open(cache_file, "w") as f:
        json.dump(cache_data, f, indent=2)

    print(f"Wrote pricing cache with {len(pricing_data)} models")


def clear_pricing_cache():
    """Remove pricing cache file."""
    cache_file = get_pricing_cache_path()

    if not cache_file.exists():
        print("Pricing cache doesn't exist - nothing to clear")
        return

    cache_file.unlink()
    print("Cleared pricing cache")


def query_bedrock_pricing(session, pricing_region: str = "us-east-1") -> dict:
    """
    Query AWS Pricing API for Bedrock model pricing.

    Args:
        session: boto3 Session
        pricing_region: AWS region for pricing API (must be us-east-1, eu-central-1, or ap-south-1)

    Returns:
        Dict mapping model names to pricing information
    """
    from botocore.exceptions import ClientError

    print(f"Querying AWS Pricing API in {pricing_region}...")

    try:
        client = session.client('pricing', region_name=pricing_region)

        # Query Bedrock pricing with pagination
        pricing_data = {}
        next_token = None
        page_count = 0

        while True:
            page_count += 1
            kwargs = {
                'ServiceCode': 'AmazonBedrockService',
                'MaxResults': 100  # Fetch all available products (we'll filter by provider later)
            }

            if next_token:
                kwargs['NextToken'] = next_token

            response = client.get_products(**kwargs)

            # Parse each product (nested JSON strings)
            for price_item_str in response.get('PriceList', []):
                price_item = json.loads(price_item_str)

                # Extract product attributes
                product = price_item.get('product', {})
                attributes = product.get('attributes', {})

                # Filter for Anthropic provider
                provider = attributes.get('provider', '')
                if provider != 'Anthropic':
                    continue

                model_name = attributes.get('model')  # Changed from 'modelName' to 'model'
                inference_type = attributes.get('inferenceType', '').lower()
                region = attributes.get('regionCode')

                if not model_name or not inference_type:
                    continue

                # Extract pricing from terms
                terms = price_item.get('terms', {})
                on_demand = terms.get('OnDemand', {})

                if not on_demand:
                    continue

                # Get first (and usually only) pricing dimension
                for term_key, term_data in on_demand.items():
                    price_dimensions = term_data.get('priceDimensions', {})
                    for dim_key, dim_data in price_dimensions.items():
                        price_per_unit = dim_data.get('pricePerUnit', {})
                        usd_price = float(price_per_unit.get('USD', 0))

                        # Initialize model entry if needed
                        if model_name not in pricing_data:
                            pricing_data[model_name] = {
                                'model_name': model_name,
                                'regions': {}
                            }

                        # Store regional pricing by inference type
                        if region not in pricing_data[model_name]['regions']:
                            pricing_data[model_name]['regions'][region] = {}

                        # Map inference type and feature type to token metric
                        # Pricing is already per 1000 tokens (unit is "1K tokens")
                        price_per_1k = usd_price
                        feature_type = attributes.get('featuretype', '').lower()

                        # Determine metric based on inference type and feature type
                        if 'cache read' in feature_type or 'cache-read' in feature_type:
                            pricing_data[model_name]['regions'][region]['cache_read_tokens_per_1k'] = price_per_1k
                        elif 'cache write' in feature_type or 'cache-write' in feature_type:
                            pricing_data[model_name]['regions'][region]['cache_write_tokens_per_1k'] = price_per_1k
                        elif 'input' in inference_type:
                            # Only set input price if not a cache operation
                            if 'cache' not in feature_type:
                                pricing_data[model_name]['regions'][region]['input_tokens_per_1k'] = price_per_1k
                        elif 'output' in inference_type:
                            pricing_data[model_name]['regions'][region]['output_tokens_per_1k'] = price_per_1k

            # Check for more pages
            next_token = response.get('NextToken')
            if not next_token:
                break

        print(f"Retrieved pricing for {len(pricing_data)} models across {page_count} page(s)")
        return pricing_data

    except ClientError as e:
        print(f"Error querying AWS Pricing API: {e}")
        raise
    except Exception as e:
        print(f"Unexpected error querying pricing: {e}")
        raise


def build_model_id_mapping(pricing_data: dict) -> dict:
    """
    Build mapping from model IDs (as they appear in logs) to pricing model names.
    Includes both API pricing data and fallback pricing models.

    Args:
        pricing_data: Pricing data from query_bedrock_pricing()

    Returns:
        Dict mapping model IDs to pricing model names
    """
    mapping = {}

    # Combine API pricing data with fallback pricing
    all_models = list(pricing_data.keys()) + list(FALLBACK_PRICING.keys())

    for model_name in all_models:
        # Generate common model ID variants
        # Example: "Claude Sonnet 4.5" -> ["claude-sonnet-4-5", "us.anthropic.claude-sonnet-4-5", etc.]

        # Normalize model name to ID format
        model_id_base = model_name.lower().replace(" ", "-").replace(".", "-")

        # Common prefixes found in logs
        prefixes = [
            "",
            "us.anthropic.",
            "global.anthropic.",
            "anthropic.",
        ]

        # Common suffixes (version/date patterns)
        suffixes = [
            "",
            "-v1:0",
            "-20250929-v1:0",
            "-20251101-v1:0",
            "-20241022-v1:0",
            "-20250514-v1:0",
            "-20251001-v1:0",  # Added for Haiku 4.5
        ]

        for prefix in prefixes:
            for suffix in suffixes:
                full_id = f"{prefix}{model_id_base}{suffix}"
                mapping[full_id] = model_name

    return mapping


def calculate_costs(usage: dict, pricing_data: dict, model_id_mapping: dict, usage_region: str = "us-east-1") -> dict:
    """
    Calculate costs for usage data and add to usage dict.

    Args:
        usage: Usage dict (will be modified in place)
        pricing_data: Pricing data from query_bedrock_pricing()
        model_id_mapping: Model ID to pricing name mapping
        usage_region: AWS region where usage occurred (for regional pricing)

    Returns:
        Extended usage dict with 'costs' added to each model and totals
    """
    missing_pricing = set()

    for arn, arn_data in usage.items():
        # Calculate per-model costs
        for model_id, model_metrics in arn_data["models"].items():
            # Look up pricing
            model_name = model_id_mapping.get(model_id)

            if not model_name or model_name not in pricing_data:
                # Try fuzzy matching: extract key parts from model_id
                # Example: "us.anthropic.claude-sonnet-4-5-20250929-v1:0" → "claude-sonnet-4"
                model_id_clean = model_id.lower().replace("anthropic.", "").replace("us.", "").replace("global.", "")

                # Try exact match with cleaned ID
                for pricing_model_name in pricing_data.keys():
                    pricing_key = pricing_model_name.lower().replace(" ", "-")
                    # Match base model name (ignore minor versions like "4-5" vs "4")
                    # Extract major version only: "claude-sonnet-4-5" → "claude-sonnet-4"
                    if pricing_key in model_id_clean or model_id_clean.startswith(pricing_key):
                        model_name = pricing_model_name
                        break

            # Get pricing (try API data first, then fallback pricing)
            regional_pricing = None

            if model_name and model_name in pricing_data:
                # Try to get regional pricing from API data
                model_pricing = pricing_data[model_name]
                regional_pricing = model_pricing['regions'].get(usage_region)

                if not regional_pricing:
                    # Use first available region as fallback
                    available_regions = list(model_pricing['regions'].keys())
                    if available_regions:
                        regional_pricing = model_pricing['regions'][available_regions[0]]

            # If no API pricing, try fallback pricing based on model name extracted from ID
            if not regional_pricing:
                # Extract human-readable model name from model_id
                from bedrock_usage_report import extract_model_name
                extracted_name = extract_model_name(model_id)

                # Try to find fallback pricing
                if extracted_name in FALLBACK_PRICING:
                    regional_pricing = FALLBACK_PRICING[extracted_name]
                    model_name = extracted_name
                else:
                    # Try fuzzy match in fallback pricing
                    for fallback_model_name in FALLBACK_PRICING.keys():
                        if fallback_model_name.lower().replace(" ", "-") in model_id.lower():
                            regional_pricing = FALLBACK_PRICING[fallback_model_name]
                            model_name = fallback_model_name
                            break

            if not regional_pricing:
                # No pricing found anywhere
                missing_pricing.add(model_id)
                model_metrics["costs"] = {
                    "input_cost": 0.0,
                    "output_cost": 0.0,
                    "cache_read_cost": 0.0,
                    "cache_write_cost": 0.0,
                    "total_cost": 0.0
                }
                continue

            # Calculate costs (tokens / 1000 * price_per_1k)
            input_cost = (model_metrics["input_tokens"] / 1000.0) * regional_pricing.get("input_tokens_per_1k", 0)
            output_cost = (model_metrics["output_tokens"] / 1000.0) * regional_pricing.get("output_tokens_per_1k", 0)
            cache_read_cost = (model_metrics["cache_read_tokens"] / 1000.0) * regional_pricing.get("cache_read_tokens_per_1k", 0)
            cache_write_cost = (model_metrics["cache_write_tokens"] / 1000.0) * regional_pricing.get("cache_write_tokens_per_1k", 0)

            total_cost = input_cost + output_cost + cache_read_cost + cache_write_cost

            model_metrics["costs"] = {
                "input_cost": input_cost,
                "output_cost": output_cost,
                "cache_read_cost": cache_read_cost,
                "cache_write_cost": cache_write_cost,
                "total_cost": total_cost
            }

        # Calculate total costs across all models
        total_input_cost = 0.0
        total_output_cost = 0.0
        total_cache_read_cost = 0.0
        total_cache_write_cost = 0.0

        for model_id, model_metrics in arn_data["models"].items():
            if "costs" in model_metrics:
                total_input_cost += model_metrics["costs"]["input_cost"]
                total_output_cost += model_metrics["costs"]["output_cost"]
                total_cache_read_cost += model_metrics["costs"]["cache_read_cost"]
                total_cache_write_cost += model_metrics["costs"]["cache_write_cost"]

        total_cost = total_input_cost + total_output_cost + total_cache_read_cost + total_cache_write_cost

        arn_data["totals"]["costs"] = {
            "total_input_cost": total_input_cost,
            "total_output_cost": total_output_cost,
            "total_cache_read_cost": total_cache_read_cost,
            "total_cache_write_cost": total_cache_write_cost,
            "total_cost": total_cost
        }

    # Report missing pricing
    if missing_pricing:
        print(f"\nWarning: No pricing found for {len(missing_pricing)} model(s):")
        for model_id in sorted(missing_pricing):
            print(f"  - {model_id}")
        print("Costs for these models set to $0.00\n")

    return usage


def get_pricing_data(
    session,
    pricing_region: str = "us-east-1",
    force_refresh: bool = False,
    ttl_hours: int = 24
) -> tuple[dict, dict]:
    """
    Get pricing data, using cache if available.

    Args:
        session: boto3 Session
        pricing_region: AWS region for pricing API
        force_refresh: Force refresh from API (bypass cache read)
        ttl_hours: Cache TTL in hours

    Returns:
        Tuple of (pricing_data, model_id_mapping)
    """
    # Try cache first (unless force refresh)
    if not force_refresh:
        cached_data = read_pricing_cache(ttl_hours)
        if cached_data:
            return cached_data["pricing"], cached_data["model_id_mapping"]

    # Query API
    try:
        pricing_data = query_bedrock_pricing(session, pricing_region)

        if not pricing_data:
            print("Warning: No pricing data returned from API")
            # Try to use stale cache as fallback
            cache_file = get_pricing_cache_path()
            if cache_file.exists():
                print("Using stale pricing cache as fallback")
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)
                return cached_data["pricing"], cached_data["model_id_mapping"]
            return {}, {}

        # Build model ID mapping
        model_id_mapping = build_model_id_mapping(pricing_data)

        # Write cache
        write_pricing_cache(pricing_data, model_id_mapping, pricing_region)

        return pricing_data, model_id_mapping

    except Exception as e:
        print(f"Error fetching pricing data: {e}")
        # Try to use stale cache as fallback
        cache_file = get_pricing_cache_path()
        if cache_file.exists():
            print("Using stale pricing cache as fallback")
            try:
                with open(cache_file, "r") as f:
                    cached_data = json.load(f)
                cached_at = cached_data.get("metadata", {}).get("cached_at", "unknown")
                print(f"Warning: Using cached pricing from {cached_at} (API query failed)")
                return cached_data["pricing"], cached_data["model_id_mapping"]
            except Exception as cache_error:
                print(f"Error reading fallback cache: {cache_error}")

        return {}, {}


# Create pricing cache directory on module import
ensure_pricing_cache_dir()
