# Usage Tracking and Billing Systems

## Per-Resource Granularity for Variable Pricing

**Context**: When building usage/billing systems where unit costs vary by resource type (e.g., AWS Bedrock models, EC2 instance types, API tiers).

**Rule**: Always store the most granular data possible (per-resource-per-user) even if you also maintain aggregates.

**Rationale**:
- You cannot retroactively break down aggregated data
- Cost structures often become more complex over time
- Different resources may have different pricing (e.g., Claude Opus costs more than Haiku)
- Cache/quota tiers may introduce additional pricing dimensions
- Business requirements for chargeback/attribution may require granular breakdowns

**Example Structure**:
```python
{
    "user_id": {
        "resources": {
            "resource_type_1": {
                "usage_metric_1": 100,
                "usage_metric_2": 200,
                "request_count": 5
            },
            "resource_type_2": {...}
        },
        "totals": {
            "total_metric_1": 300,
            "total_metric_2": 400,
            "total_requests": 10,
            "resources_used": ["type_1", "type_2"]
        }
    }
}
```

**Anti-pattern**:
```python
# BAD: Only aggregated totals with list of resources
{
    "user_id": {
        "total_usage": 1000,
        "resources_used": ["resource_1", "resource_2"]  # Can't calculate per-resource costs!
    }
}
```

**Benefits of Dual Structure**:
- Granular data enables accurate cost calculation
- Totals provide fast summary queries
- Can always compute totals from granular data, but not vice versa
- Supports evolving business requirements without data migration
