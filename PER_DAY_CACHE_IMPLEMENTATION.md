# Per-Day Granular Caching Implementation

## Summary

Successfully implemented per-day caching granularity for the Bedrock usage report tool. The cache system now stores data by individual days instead of full date ranges, enabling efficient cache reuse across different queries.

## Changes Made

### 1. `cache_manager.py` - Complete Rewrite

**New functions:**
- `get_log_group_hash(log_group)` - Generate short hash from log group identifier
- `get_cache_dir(cache_type, source, region, log_group)` - Get cache directory path
- `get_day_cache_file(...)` - Get cache file path for a specific day
- `generate_date_list(start_date, end_date)` - Generate list of dates in range
- `is_today(date_str)` - Check if a date is today (UTC)
- `read_full_cache_for_range(...)` - Read full cache for date range, return events and missing dates
- `write_full_cache_by_day(...)` - Write full cache organized by day with optional append mode
- `read_summary_cache_for_range(...)` - Read summary cache for date range, merge usage data
- `write_summary_cache_by_day(...)` - Write summary cache organized by day
- `merge_usage_dicts(usage1, usage2)` - Merge two usage dictionaries

**Replaced functions:**
- Removed: `get_cache_key()`, `read_full_cache()`, `write_full_cache()`, `read_summary_cache()`, `write_summary_cache()`
- These were single-file range-based functions that are no longer needed

**Updated functions:**
- `clear_full_cache()` - Now uses recursive glob to find all JSON files
- `clear_all_cache()` - Updated to count files recursively before deletion

### 2. `bedrock_usage_report.py` - Updated Main Logic

**New function:**
- `split_events_by_day(events)` - Split log events by day based on timestamp

**Updated imports:**
- Replaced old cache functions with new per-day functions
- Added `generate_date_list` import

**Rewritten `main()` function:**
- Now checks summary cache per-day first
- Falls back to full cache per-day if summary cache has missing dates
- Queries AWS only for truly missing dates
- Supports incremental updates for today (using `last_fetch_timestamp`)
- Splits new events by day before caching
- Handles append mode for incremental updates
- Better progress messages showing cached vs queried days

### 3. New Cache Directory Structure

**Before:**
```
.cache/
├── full/
│   └── {cache_key_hash}.json
└── summary/
    └── {cache_key_hash}.json
```

**After:**
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

### 4. Incremental Today Updates

**Special handling for current day:**
- Today's cache includes `last_fetch_timestamp` in metadata
- When querying today's data again, only logs after the last fetch are retrieved
- New logs are appended to existing cache instead of overwriting
- Once a day becomes yesterday, it's treated as complete and never re-queried

**Incremental update flow:**
1. Query at 10:00 → Caches today's data, stores `last_fetch_timestamp = 10:00`
2. Query at 15:00 → Queries only logs after 10:00, appends to cache, updates timestamp to 15:00
3. Query tomorrow → Yesterday's cache is complete, today starts fresh

### 5. Test Coverage

Created `test_per_day_cache.py` with tests for:
- Date list generation
- Per-day full cache write and read
- Per-day summary cache write and read
- Partial cache hits (some days cached, some missing)
- Cache directory structure validation

All tests pass successfully.

### 6. Documentation Updates

Updated `CLAUDE.md` to explain:
- Per-day caching benefits
- How cache reuse works across different date ranges
- Cache directory structure
- Incremental today updates

## Key Benefits

### 1. Cache Reuse Across Date Ranges
**Before:**
- Query Jan 1-31 → Cache file A
- Query Jan 25-31 → Cache file B (duplicate data for Jan 25-31)

**After:**
- Query Jan 1-31 → 31 cache files (one per day)
- Query Jan 25-31 → Reuses 7 existing cache files, no AWS query

### 2. Efficient Storage
- No duplicate data across overlapping queries
- Each day cached only once regardless of query patterns

### 3. Incremental Updates
- Add new days without touching old days
- Today's data updates incrementally without re-downloading

### 4. Flexible Querying
- Query any date range and automatically reuse all available cached data
- Partial cache hits are valuable (query only what's missing)

## Example Usage Scenarios

### Scenario 1: Fresh Query
```bash
python bedrock_usage_report.py --start-date 2025-01-01 --end-date 2025-01-07 --profile aytm
```
**Result:** Queries AWS for 7 days, creates 7 cache files

### Scenario 2: Subset Reuse
```bash
python bedrock_usage_report.py --start-date 2025-01-03 --end-date 2025-01-05 --profile aytm
```
**Result:** Uses 3 cached files instantly, no AWS query

### Scenario 3: Partial Cache Hit
```bash
python bedrock_usage_report.py --start-date 2025-01-05 --end-date 2025-01-10 --profile aytm
```
**Result:**
- Reuses cached days: Jan 5-7 (3 days)
- Queries AWS for: Jan 8-10 (3 days)
- Output: "Partial cache hit from s3: 3 days cached, 3 days to query"

### Scenario 4: Incremental Daily Updates
```bash
# Morning
python bedrock_usage_report.py --start-date 2025-02-04 --end-date 2025-02-04 --profile aytm

# Evening (same day)
python bedrock_usage_report.py --start-date 2025-02-04 --end-date 2025-02-04 --profile aytm
```
**Result:**
- First run: Queries full day, stores `last_fetch_timestamp`
- Second run: Queries only new logs since morning, appends to cache
- Output: "Incremental update for s3: querying 1 days since last fetch"

## Backward Compatibility

- Old cache files (`.cache/full/{hash}.json`) are ignored by new code
- No migration needed - new queries create new per-day cache files
- Old cache can be cleaned up with `--clear-all-cache`

## Edge Cases Handled

1. **Empty days**: Days with no logs create cache files with empty events array
2. **Clock skew**: Events grouped by UTC timestamp date
3. **Missing timestamps**: Events without timestamp grouped under "1970-01-01"
4. **Concurrent queries**: Each day is independent, no locking needed
5. **Cache corruption**: Single day corruption doesn't affect other days
6. **Today handling**: Always treated as incomplete for incremental updates

## Performance Impact

### Storage
- **Improved**: No duplicate data across overlapping queries
- **Trade-off**: More files (one per day) vs fewer large files

### Speed
- **Improved**: Instant cache hits for any date range with cached data
- **Improved**: Partial cache hits reduce AWS API calls
- **Improved**: Today's incremental updates avoid re-downloading same data

### AWS API Calls
- **Dramatically reduced**: Only query missing days
- **Example**: Monthly report run daily only queries 1 new day each time

## Testing

Run tests with:
```bash
source venv/bin/activate
python test_per_day_cache.py
```

All tests pass:
- ✓ generate_date_list works correctly
- ✓ Cache files created correctly
- ✓ Read and merge cached data
- ✓ Partial cache hits work
- ✓ Cache directory structure correct

## Future Enhancements (Not Implemented)

Potential improvements for future consideration:
1. Cache expiration for very old data
2. Cache compression for large datasets
3. Cache statistics and management commands
4. Parallel processing for multi-day queries
