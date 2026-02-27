# Cache and Persisted Data Format Evolution

## Backward Compatibility for Data Structure Changes

**Context**: When modifying cached or persisted data structures (files, databases, serialized objects).

**Rule**: Make readers backward-compatible by checking for new field existence rather than assuming a fixed schema.

**Implementation Strategy**:

1. **Defensive Reading**: Check for field presence before accessing
```python
# Handle both old and new cache format
if "new_field" in data:
    # New format
    process_new_format(data)
else:
    # Old format - skip or convert
    handle_old_format(data)
```

2. **Decide Early**: Choose migration strategy upfront
   - **Skip old data**: Accept data loss, simpler implementation
   - **Migrate on read**: Convert old format to new, more complex
   - **Explicit migration**: One-time conversion script

3. **Version Fields**: Consider adding explicit versioning for complex migrations
```python
cache_data = {
    "version": "2.0",  # Enables version-specific handling
    "metadata": {...},
    "data": {...}
}
```

4. **Graceful Degradation**: Never crash on old data
```python
try:
    data = load_cache()
    if is_compatible(data):
        return parse_data(data)
    else:
        logger.info("Skipping incompatible cache entry")
        return None  # Triggers fresh query
except (KeyError, ValueError):
    return None  # Handle corrupt data gracefully
```

**Benefits**:
- Users don't need to clear cache after updates
- Gradual migration as cache entries expire naturally
- Reduced support burden
- Better user experience during upgrades

**Anti-pattern**:
```python
# BAD: Assumes fixed structure
def read_cache(data):
    return data["new_field"]["nested_field"]  # Crashes on old data
```

**When to Use Each Strategy**:
- **Skip old data**: Non-critical cache, easy to regenerate, simple implementation
- **Migrate on read**: Critical data, expensive to regenerate, worth complexity
- **Explicit migration**: Database schemas, breaking changes, need guarantees

**Documentation**: Always document cache format in project CLAUDE.md for future reference.
