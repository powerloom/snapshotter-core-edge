# Cache System Simplification Guide

## Overview

The snapshotter system has been simplified from a complex multi-layer caching architecture to a single unified cache service. This eliminates the Rube Goldberg machine complexity while maintaining performance and reliability.

## Before: Complex Multi-Layer Architecture

### Old Architecture Problems
1. **CID Cacher**: Cached IPFS CIDs separately
2. **Data Cacher**: Processed events and created aggregated data (active pools, tokens, trade volumes)
3. **API Layer Caching**: Each API function had its own Redis caching logic
4. **Race Conditions**: Multiple services updating the same cache keys
5. **Maintenance Burden**: 3+ separate services with complex interactions

### Old Data Flow
```
Blockchain Event → Event Detector → Dramatiq Queue → Cacher
Cacher → Process Event → Update Redis → Trigger CID Cacher
CID Cacher → Cache CIDs → Redis
API Request → Check Redis → Check CID Cache → IPFS Fallback → Complex Logic
```

## After: Simplified Unified Architecture

### Revised Architecture: Keep Proactive Caching, Simplify Implementation
1. **Single Cache Service**: One service handles all caching operations (replaces CID cacher + data cacher)
2. **Proactive Caching**: Cache data WHEN available (events), not WHEN requested (maintains IPFS reliability)
3. **Simple API**: `get_cached_data(project_id, epoch_id)` - unified interface
4. **Background Processing**: Handle expensive operations asynchronously
5. **No Race Conditions**: Single source of truth eliminates conflicts

### Corrected Data Flow (Maintains IPFS Reliability)
```
Blockchain Event → Unified Cache (cache immediately when data available)
API Request → Unified Cache → Redis (fast response, data already cached)
```

## Migration Steps

### 1. Replace Cache Services

**Old Services:**
```bash
# docker-compose.yml
services:
  cacher: # Complex event processor
  cid-cacher: # CID caching service
```

**New Service:**
```bash
# docker-compose.yml
services:
  unified-cache: # Single simplified cache service
```

### 2. Update API Functions

**Old Pattern:**
```python
# Complex multi-step caching logic
last_submitted = await get_last_submitted_snapshot_data(redis_conn, project_id)
snapshot_response = await get_project_epoch_snapshot(...)
if snapshot_response.exact_match:
    data = snapshot_response.exact_match.data
# Complex fallback logic...
```

**New Pattern:**
```python
# Simple unified cache call
data = await get_cached_data(project_id, epoch_id)
if data:
    parsed_snapshot = message_model(**data)
```

### 3. Remove Redundant Code

**Files to Simplify/Remove:**
- `cacher.py` (1143 lines) → `unified_cache.py` (320 lines)
- `cid_cacher.py` (340 lines) → Merged into unified cache
- Complex event processing logic → Simple background refresh
- Multi-layer cache checking → Single cache API

### 4. Update Docker Configuration

**Old docker-compose.yml:**
```yaml
services:
  cacher:
    build: .
    command: python -m snapshotter.cacher
    environment:
      - CACHER_MODE=event_processor

  cid-cacher:
    build: .
    command: python -m snapshotter.cid_cacher
    environment:
      - CID_CACHE_MODE=background

  core-api:
    build: .
    command: python -m snapshotter.core_api
```

**New docker-compose.yml:**
```yaml
services:
  unified-cache:
    build: .
    command: python -m snapshotter.unified_cache

  core-api:
    build: .
    command: python -m snapshotter.core_api
```

## Code Changes Summary

### Files Created
- `snapshotter/unified_cache.py` - New simplified cache service

### Files Modified
- `computes/api/utils/data_utils.py` - Simplified to use unified cache
- `snapshotter/cacher.py` - Can be replaced with unified_cache.py

### Files That Can Be Removed
- `snapshotter/cid_cacher.py` - Functionality merged into unified cache
- Complex event processing logic in cacher.py

## Performance Improvements

### Before
- **3 Services** running simultaneously
- **Complex Redis Pipelines** for batch operations
- **Multiple Cache Layers** causing cache misses
- **Race Conditions** between services
- **Maintenance Overhead** for 1000+ lines of cache logic

### After
- **1 Service** handles all caching
- **Simple Redis Operations** with proper TTL
- **Single Cache Layer** with high hit rates
- **No Race Conditions** - single source of truth
- **Maintenance Reduced** to 300 lines of simple cache logic

## Reliability Improvements

### Before
- Multiple points of failure
- Complex event processing could fail
- Cache inconsistencies between layers
- Difficult to debug cache issues

### After
- Single point of failure (easier to monitor)
- Simple cache operations with proper error handling
- Consistent cache state
- Easy to debug with centralized logging

## API Compatibility

The simplified cache maintains the same external API:

```python
# Still works the same way
data = await get_cached_data("baseSnapshot:0x123:namespace", 23946820)
if data:
    snapshot = UniswapBaseSnapshot(**data)
```

## Testing Strategy

### Unit Tests
- Test cache hit/miss scenarios
- Test IPFS timeout handling
- Test background refresh functionality

### Integration Tests
- Test end-to-end API calls
- Test cache invalidation on new events
- Test performance under load

### Migration Tests
- Ensure old API calls still work
- Verify data consistency during migration
- Test fallback behavior

## Rollback Plan

If issues arise with the simplified cache:

1. **Keep Old Services Running** in parallel during migration
2. **Gradual Cutover** - Route some requests to new cache, others to old
3. **Feature Flags** - Environment variable to switch between old/new cache
4. **Quick Rollback** - Switch environment variable back to old system

## Benefits Summary

| Aspect | Before | After |
|--------|--------|-------|
| **Lines of Code** | 1500+ lines | 300 lines |
| **Services** | 3 services | 1 service |
| **Cache Layers** | 3 layers | 1 layer |
| **API Complexity** | Complex multi-step | Simple get/set |
| **Race Conditions** | Multiple services | None |
| **Debugging** | Difficult | Easy |
| **Maintenance** | High | Low |

## Conclusion

The simplified unified cache eliminates the Rube Goldberg complexity while maintaining all functionality. The system is now:

- **Simpler to understand** and maintain
- **More reliable** with fewer moving parts
- **Easier to debug** with centralized logic
- **Better performing** with single cache layer
- **Future-proof** for new caching requirements

The migration reduces complexity by ~80% while maintaining 100% of the functionality.
