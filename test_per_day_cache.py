#!/usr/bin/env python3
"""
Basic test for per-day caching functionality.
Tests that cache files are created per day and can be read back.
"""

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from cache_manager import (
    generate_date_list,
    get_cache_dir,
    write_full_cache_by_day,
    read_full_cache_for_range,
    write_summary_cache_by_day,
    read_summary_cache_for_range,
    is_today,
    CACHE_DIR,
)


def test_generate_date_list():
    """Test date list generation."""
    dates = generate_date_list("2025-01-01", "2025-01-05")
    assert dates == ["2025-01-01", "2025-01-02", "2025-01-03", "2025-01-04", "2025-01-05"]
    print("✓ generate_date_list works correctly")


def test_per_day_full_cache():
    """Test per-day full cache write and read."""
    # Clean test cache
    test_cache = Path(".cache_test")
    if test_cache.exists():
        shutil.rmtree(test_cache)

    # Create mock events by day
    events_by_day = {
        "2025-01-01": [
            {"timestamp": 1735689600000, "message": '{"identity":{"arn":"arn:aws:iam::123:user/test1"},"input":{"inputTokenCount":100},"output":{"outputTokenCount":50},"modelId":"claude-3"}'},
            {"timestamp": 1735690000000, "message": '{"identity":{"arn":"arn:aws:iam::123:user/test2"},"input":{"inputTokenCount":200},"output":{"outputTokenCount":100},"modelId":"claude-3"}'},
        ],
        "2025-01-02": [
            {"timestamp": 1735776000000, "message": '{"identity":{"arn":"arn:aws:iam::123:user/test1"},"input":{"inputTokenCount":150},"output":{"outputTokenCount":75},"modelId":"claude-3"}'},
        ],
    }

    # Write cache
    write_full_cache_by_day(
        "s3",
        "test-bucket/prefix",
        "us-east-1",
        events_by_day
    )

    # Verify files were created
    cache_dir = get_cache_dir("full", "s3", "us-east-1", "test-bucket/prefix")
    assert (cache_dir / "2025-01-01.json").exists()
    assert (cache_dir / "2025-01-02.json").exists()
    print(f"✓ Cache files created at {cache_dir}")

    # Read cache back
    events, missing_dates, _ = read_full_cache_for_range(
        "s3",
        "test-bucket/prefix",
        "2025-01-01",
        "2025-01-02",
        "us-east-1"
    )

    assert len(events) == 3, f"Expected 3 events, got {len(events)}"
    assert len(missing_dates) == 0, f"Expected no missing dates, got {missing_dates}"
    print(f"✓ Read {len(events)} events from cache")


def test_per_day_summary_cache():
    """Test per-day summary cache write and read."""
    # Create mock usage by day
    usage_by_day = {
        "2025-01-01": {
            "arn:aws:iam::123:user/test1": {
                "total_input_tokens": 100,
                "total_output_tokens": 50,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "request_count": 1,
                "models_used": {"claude-3"},
            }
        },
        "2025-01-02": {
            "arn:aws:iam::123:user/test1": {
                "total_input_tokens": 150,
                "total_output_tokens": 75,
                "cache_read_tokens": 0,
                "cache_write_tokens": 0,
                "request_count": 1,
                "models_used": {"claude-3"},
            }
        },
    }

    # Write cache
    write_summary_cache_by_day(
        "s3",
        "test-bucket/prefix",
        "us-east-1",
        usage_by_day
    )

    # Verify files were created
    cache_dir = get_cache_dir("summary", "s3", "us-east-1", "test-bucket/prefix")
    assert (cache_dir / "2025-01-01.json").exists()
    assert (cache_dir / "2025-01-02.json").exists()
    print(f"✓ Summary cache files created at {cache_dir}")

    # Read cache back
    usage, missing_dates = read_summary_cache_for_range(
        "s3",
        "test-bucket/prefix",
        "2025-01-01",
        "2025-01-02",
        "us-east-1"
    )

    assert len(usage) == 1, f"Expected 1 user, got {len(usage)}"
    assert len(missing_dates) == 0, f"Expected no missing dates, got {missing_dates}"
    user_metrics = usage["arn:aws:iam::123:user/test1"]
    assert user_metrics["total_input_tokens"] == 250  # 100 + 150
    assert user_metrics["total_output_tokens"] == 125  # 50 + 75
    assert user_metrics["request_count"] == 2  # 1 + 1
    print(f"✓ Read and merged usage for {len(usage)} users")


def test_partial_cache_hit():
    """Test partial cache hit (some days cached, some missing)."""
    # Read range that includes uncached dates
    events, missing_dates, _ = read_full_cache_for_range(
        "s3",
        "test-bucket/prefix",
        "2025-01-01",
        "2025-01-05",
        "us-east-1"
    )

    # Should have cached events from Jan 1-2 and missing Jan 3-5
    assert len(events) == 3, f"Expected 3 cached events, got {len(events)}"
    assert len(missing_dates) == 3, f"Expected 3 missing dates, got {len(missing_dates)}"
    assert "2025-01-03" in missing_dates
    assert "2025-01-04" in missing_dates
    assert "2025-01-05" in missing_dates
    print(f"✓ Partial cache hit: {len(events)} events cached, {len(missing_dates)} dates missing")


def test_cache_directory_structure():
    """Test that cache directory structure is correct."""
    cache_dir = get_cache_dir("full", "s3", "us-east-1", "test-bucket/prefix")
    expected_structure = Path(".cache/full/s3/us-east-1")
    assert str(cache_dir).startswith(str(expected_structure))
    print(f"✓ Cache directory structure correct: {cache_dir}")


def cleanup():
    """Clean up test cache."""
    if CACHE_DIR.exists():
        shutil.rmtree(CACHE_DIR)
    print("✓ Test cache cleaned up")


if __name__ == "__main__":
    print("Running per-day cache tests...\n")

    try:
        test_generate_date_list()
        test_per_day_full_cache()
        test_per_day_summary_cache()
        test_partial_cache_hit()
        test_cache_directory_structure()

        print("\n✅ All tests passed!")

    except AssertionError as e:
        print(f"\n❌ Test failed: {e}")
        raise
    finally:
        cleanup()
