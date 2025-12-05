"""
Unified Cache Service - Corrected Approach

Maintains proactive caching (cache WHEN data is available via events) while
simplifying the architecture. The key improvements:

1. Single caching layer instead of CID cacher + data cacher + API caching
2. Proactive caching: Cache immediately when blockchain events provide data
3. Simple get/set API for data access
4. Eliminates Rube Goldberg complexity while maintaining IPFS reliability

Why NOT cache-on-demand: IPFS infrastructure is unreliable, slow, and data
availability isn't guaranteed. APIs need fast responses, so we cache proactively
when data is available (during events) rather than when requested.

Architecture:
- Listens to blockchain events and caches data immediately
- Provides simple cache API for fast data retrieval
- Handles all caching logic in one place
- Background processing for expensive operations
"""

import json
import asyncio
import multiprocessing
import threading
import time
import traceback
from typing import Dict, List, Set, Optional, Tuple, Any
from uuid import uuid4
from ipfs_client.main import AsyncIPFSClientSingleton
from ipfs_client.dag import IPFSAsyncClientError
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import cid_cache, project_data_hmap, last_submitted_snapshot_data_key, data_expiry_zset, project_last_finalized_epoch_hmap, cids_to_cache_set
from snapshotter.utils.data_utils import get_submission_data, PROJECT_DATA_ENTRY_EXPIRY, get_project_config


class UnifiedCache(multiprocessing.Process):
    """
    Simplified Unified Cache Service

    Replaces the complex multi-layer caching system with a single service that:
    - Handles all data caching (replaces CID cacher + data cacher)
    - Uses cache-on-demand strategy with background refresh
    - Provides simple get/set API for data access
    - Eliminates redundant caching layers and complexity
    """

    def __init__(self, name, **kwargs):
        super().__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + str(uuid4())[:8]
        self._logger = default_logger.bind(module=f'UnifiedCache:{settings.namespace}')
        self._shutdown_initiated = False

        # Core components
        self._aioredis_pool: Optional[RedisPoolCache] = None
        self._redis_conn: Optional[aioredis.Redis] = None
        self._ipfs_singleton: Optional[AsyncIPFSClientSingleton] = None
        self._ipfs_reader_client = None

        # Cache performance tracking
        self._cache_hit_stats: Dict[str, int] = {}
        self._cache_miss_stats: Dict[str, int] = {}

        # Active tasks for background processing
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
        self._task_cleanup_interval = 30  # seconds

        # Event processing
        self._event_queue = asyncio.Queue()
        self._processed_epochs: Set[int] = set()  # Track processed epochs to avoid duplicates

    async def _init_components(self):
        """Initialize Redis and IPFS connections"""
        # Redis
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

        # IPFS
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
        await self._ipfs_singleton.init_sessions()
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

        self._logger.info("Unified cache components initialized")

    async def get_cached_data(self, project_id: str, epoch_id: Optional[int] = None) -> Optional[Dict]:
        """
        Get cached data for a project/epoch combination.

        This is the main API for data retrieval - simple and unified.
        Data should already be cached proactively when events arrived.

        Args:
            project_id: The project identifier
            epoch_id: Optional epoch ID, uses latest if None

        Returns:
            Cached data dict or None if not found
        """
        try:
            # Determine target epoch
            if epoch_id is None:
                epoch_id = await self._get_latest_epoch_id(project_id)
                if epoch_id is None:
                    return None

            # Check Redis cache (should be populated by proactive caching)
            cache_key = f"cache:{project_id}:{epoch_id}:{settings.namespace}"
            cached_data = await self._redis_conn.get(cache_key)

            if cached_data:
                self._cache_hit_stats[project_id] = self._cache_hit_stats.get(project_id, 0) + 1
                return json.loads(cached_data)

            # Cache miss - data wasn't proactively cached
            # This could happen for historical data or system issues
            self._cache_miss_stats[project_id] = self._cache_miss_stats.get(project_id, 0) + 1
            self._logger.warning(f"Cache miss for {project_id}:{epoch_id} - data not proactively cached")

            # Attempt to fetch and cache on-demand as fallback (but log the issue)
            data = await self._fetch_and_cache_data(project_id, epoch_id)
            if data:
                self._logger.info(f"Successfully recovered missing data for {project_id}:{epoch_id}")
            else:
                self._logger.error(f"Failed to recover missing data for {project_id}:{epoch_id}")

            return data

        except Exception as e:
            self._logger.error(f"Error getting cached data for {project_id}:{epoch_id}: {e}")
            return None

    async def _get_latest_epoch_id(self, project_id: str) -> Optional[int]:
        """Get the latest available epoch ID for a project"""
        try:
            # Check last submitted snapshot data
            last_submitted_key = last_submitted_snapshot_data_key(project_id)
            last_data = await self._redis_conn.get(last_submitted_key)

            if last_data:
                data = json.loads(last_data)
                return data.get('epochId')

            # Fallback to project data hashmap
            project_hmap = project_data_hmap(project_id=project_id)
            epoch_data = await self._redis_conn.hgetall(project_hmap)

            if epoch_data:
                # Get the highest epoch ID
                epochs = [int(epoch) for epoch in epoch_data.keys()]
                return max(epochs) if epochs else None

            return None

        except Exception as e:
            self._logger.error(f"Error getting latest epoch for {project_id}: {e}")
            return None

    async def _fetch_and_cache_data(self, project_id: str, epoch_id: int) -> Optional[Dict]:
        """Fetch data from IPFS and cache it"""
        try:
            # Get CID from project data
            project_hmap = project_data_hmap(project_id=project_id)
            epoch_data_raw = await self._redis_conn.hget(project_hmap, str(epoch_id))

            if not epoch_data_raw:
                return None

            epoch_data = json.loads(epoch_data_raw)
            cid = epoch_data.get('snapshot_cid')

            if not cid or 'null' in cid:
                return None

            # Fetch from IPFS with timeout
            data = await self._fetch_from_ipfs_with_timeout(cid)
            if not data:
                return None

            # Cache the result
            cache_key = f"cache:{project_id}:{epoch_id}:{settings.namespace}"
            await self._redis_conn.set(
                cache_key,
                json.dumps(data),
                ex=PROJECT_DATA_ENTRY_EXPIRY
            )

            # Also cache the CID for faster future lookups
            cid_cache_key = cid_cache(cid)
            await self._redis_conn.set(
                cid_cache_key,
                json.dumps(data),
                ex=PROJECT_DATA_ENTRY_EXPIRY
            )

            self._logger.debug(f"Cached data for {project_id}:{epoch_id} (CID: {cid})")
            return data

        except Exception as e:
            self._logger.error(f"Error fetching and caching data for {project_id}:{epoch_id}: {e}")
            return None

    async def _fetch_from_ipfs_with_timeout(self, cid: str) -> Optional[Dict]:
        """Fetch data from IPFS with proper timeout and error handling"""
        try:
            # Add timeout to prevent hanging
            data = await asyncio.wait_for(
                self._ipfs_reader_client.cat(cid),
                timeout=30.0  # 30 second timeout
            )

            if isinstance(data, bytes):
                data = data.decode('utf-8')

            return json.loads(data)

        except asyncio.TimeoutError:
            self._logger.warning(f"IPFS fetch timeout for CID {cid}")
            return None
        except IPFSAsyncClientError as e:
            # Permanent IPFS error
            self._logger.warning(f"IPFS client error for CID {cid}: {e}")
            return None
        except Exception as e:
            self._logger.warning(f"Unexpected IPFS error for CID {cid}: {e}")
            return None

    async def invalidate_cache(self, project_id: str, epoch_id: Optional[int] = None):
        """Invalidate cache for a project/epoch"""
        try:
            if epoch_id:
                # Invalidate specific epoch
                cache_key = f"cache:{project_id}:{epoch_id}:{settings.namespace}"
                await self._redis_conn.delete(cache_key)
                self._logger.info(f"Invalidated cache for {project_id}:{epoch_id}")
            else:
                # Invalidate all epochs for project (more expensive)
                pattern = f"cache:{project_id}:*:{settings.namespace}"
                # Note: Redis doesn't have native pattern deletion, would need SCAN in production
                self._logger.warning(f"Full project cache invalidation not implemented for {project_id}")

        except Exception as e:
            self._logger.error(f"Error invalidating cache for {project_id}:{epoch_id}: {e}")

    async def handle_event(self, event_type: str, event_data: Dict):
        """
        Handle blockchain events and proactively cache data.

        This replaces the complex event processing logic with simple,
        proactive caching when data becomes available.
        """
        try:
            if event_type == "SnapshotSubmitted":
                await self._handle_snapshot_submitted(event_data)
            elif event_type == "SnapshotFinalized":
                await self._handle_snapshot_finalized(event_data)
            elif event_type == "SnapshotBatchSubmitted":
                await self._handle_snapshot_batch_submitted(event_data)
            else:
                self._logger.debug(f"Ignoring unhandled event type: {event_type}")

        except Exception as e:
            self._logger.error(f"Error handling event {event_type}: {e}")

    async def _handle_snapshot_submitted(self, event_data: Dict):
        """Handle SnapshotSubmitted event - equivalent to original cacher logic"""
        try:
            snapshot_cid = event_data.get("snapshotCid")
            epoch_id = event_data.get("epochId")
            project_id = event_data.get("projectId")

            if not all([snapshot_cid, epoch_id, project_id]):
                self._logger.warning(f"Incomplete SnapshotSubmitted event data: {event_data}")
                return

            # Avoid processing duplicates
            epoch_key = f"{project_id}:{epoch_id}"
            if epoch_key in self._processed_epochs:
                return

            self._processed_epochs.add(epoch_key)

            # Process snapshot CID equivalent to original process_snapshot_cid
            await self._process_snapshot_cid(project_id, snapshot_cid, epoch_id, epoch_id)

            # Update project hashmap with this snapshot
            project_hmap_key = project_data_hmap(project_id=project_id)
            await self._redis_conn.hset(
                project_hmap_key,
                str(epoch_id),
                json.dumps({
                    'snapshot_cid': snapshot_cid,
                    'status': 'SUBMITTED'
                })
            )

            self._logger.debug(f"Processed snapshot for {project_id}:{epoch_id} (CID: {snapshot_cid[:16]}...)")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotSubmitted event: {e}")

    async def _handle_snapshot_finalized(self, event_data: Dict):
        """Handle SnapshotFinalized event - mark epoch as finalized"""
        try:
            epoch_id = event_data.get("epochId")
            project_id = event_data.get("projectId")

            if not epoch_id or not project_id:
                return

            # Update project last finalized epoch
            project_last_finalized_key = project_last_finalized_epoch_hmap(project_id)
            await self._redis_conn.hset(project_last_finalized_key, project_id, epoch_id)

            self._logger.debug(f"Marked epoch {epoch_id} as finalized for {project_id}")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotFinalized event: {e}")

    async def _handle_snapshot_batch_submitted(self, event_data: Dict):
        """Handle SnapshotBatchSubmitted event - cache batch snapshots"""
        try:
            project_ids = event_data.get("projectIds", [])
            snapshot_cids = event_data.get("snapshotCids", [])
            epoch_id = event_data.get("epochId")

            if not all([project_ids, snapshot_cids, epoch_id]):
                return

            # Process each snapshot in the batch
            for project_id, snapshot_cid in zip(project_ids, snapshot_cids):
                epoch_key = f"{project_id}:{epoch_id}"
                if epoch_key not in self._processed_epochs:
                    self._processed_epochs.add(epoch_key)
                    # Process snapshot CID (equivalent to original cacher logic)
                    await self._process_snapshot_cid(project_id, snapshot_cid, epoch_id, epoch_id)

                    # Update project hashmap with this snapshot
                    project_hmap_key = project_data_hmap(project_id=project_id)
                    await self._redis_conn.hset(
                        project_hmap_key,
                        str(epoch_id),
                        json.dumps({
                            'snapshot_cid': snapshot_cid,
                            'status': 'SUBMITTED'
                        })
                    )

            self._logger.debug(f"Processed batch of {len(project_ids)} snapshots for epoch {epoch_id}")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotBatchSubmitted event: {e}")

    async def _process_snapshot_cid(self, project_id: str, snapshot_cid: str, epoch_id: int, original_epoch_id: int):
        """Process snapshot CID equivalent to original process_snapshot_cid function"""
        try:
            if 'null' in snapshot_cid:
                self._logger.info(f"Snapshot cid is null for project {project_id} at epoch {epoch_id}")
                return False

            self._logger.info(f"Processing snapshot cid: {snapshot_cid} for project {project_id} at epoch {epoch_id}")

            # Get project config to check if we should keep previous snapshot data
            from snapshotter.utils.data_utils import get_project_config
            project_config = get_project_config(project_id)
            if not project_config.keep_previous_snapshot_data:
                return False

            # Fetch snapshot data from IPFS
            snapshot_data = await get_submission_data(snapshot_cid, self._ipfs_reader_client, False)

            if snapshot_data:
                pipeline = self._redis_conn.pipeline()
                expiry_keys = []
                project_hmap_key = project_data_hmap(project_id=project_id)
                expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY

                # Process previous snapshots if they exist
                if "previousSnapshots" in snapshot_data and len(snapshot_data["previousSnapshots"]) > 0:
                    data_to_cache = {}
                    all_previous_snapshot_cids = set()

                    # Process each previous snapshot
                    for (prev_epoch_id, prev_snapshot_cid) in snapshot_data["previousSnapshots"][::-1]:
                        prev_epoch_id = int(prev_epoch_id)
                        data_to_cache[str(prev_epoch_id)] = json.dumps({
                            "snapshot_cid": prev_snapshot_cid,
                            "status": "SUBMITTED"
                        })
                        all_previous_snapshot_cids.add(prev_snapshot_cid)
                        expiry_keys.append(f"{project_id}|{prev_epoch_id}")

                    # Cache all previous snapshot metadata
                    if data_to_cache:
                        pipeline.hset(project_hmap_key, mapping=data_to_cache)

                    # Cache CIDs if configured
                    if all_previous_snapshot_cids and project_config.cache_cids:
                        for cid in all_previous_snapshot_cids:
                            cid_cache_key = cid_cache(cid)
                            pipeline.set(cid_cache_key, json.dumps(snapshot_data), ex=PROJECT_DATA_ENTRY_EXPIRY)

                # Execute the pipeline
                if pipeline:
                    await pipeline.execute()

                # Add expiry keys to zset for cleanup
                if expiry_keys:
                    pipeline = self._redis_conn.pipeline()
                    for key in expiry_keys:
                        pipeline.zadd(data_expiry_zset, {key: expiry_time})
                    await pipeline.execute()

            return True

        except Exception as e:
            self._logger.error(f"Error processing snapshot CID {snapshot_cid}: {e}")
            return False

    async def refresh_cache_background(self, project_id: str, epoch_id: int):
        """Background task to refresh cache when needed"""
        try:
            # Create tracked task for background processing
            task = asyncio.create_task(self._fetch_and_cache_data(project_id, epoch_id))
            self._active_tasks.add((time.time(), task))

            # Wait for completion but don't block caller
            await task

        except Exception as e:
            self._logger.error(f"Error in background cache refresh for {project_id}:{epoch_id}: {e}")

    async def _cleanup_tasks(self):
        """Clean up completed/finished tasks"""
        current_time = time.time()
        completed_tasks = []

        for start_time, task in self._active_tasks:
            if task.done() or (current_time - start_time) > 300:  # 5 minute timeout
                completed_tasks.append((start_time, task))

        for start_time, task in completed_tasks:
            self._active_tasks.remove((start_time, task))
            if not task.done():
                task.cancel()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        total_hits = sum(self._cache_hit_stats.values())
        total_misses = sum(self._cache_miss_stats.values())
        total_requests = total_hits + total_misses

        return {
            'total_requests': total_requests,
            'total_hits': total_hits,
            'total_misses': total_misses,
            'hit_rate': total_hits / total_requests if total_requests > 0 else 0,
            'project_stats': {
                'hits': self._cache_hit_stats,
                'misses': self._cache_miss_stats
            },
            'active_tasks': len(self._active_tasks)
        }

    async def run(self):
        """Main run loop for the cache service"""
        await self._init_components()

        # Start periodic CID caching task (equivalent to original cid_cacher)
        cid_cache_task = asyncio.create_task(self._periodic_cid_caching())
        self._active_tasks.add((time.time(), cid_cache_task))

        self._logger.info("Unified cache service started")

        try:
            while not self._shutdown_initiated:
                try:
                    # Process any queued events
                    await self._process_event_queue()

                    # Periodic cleanup of background tasks
                    await self._cleanup_tasks()

                    # Periodic cleanup of processed epochs set (prevent memory growth)
                    await self._cleanup_processed_epochs()

                    # Log stats periodically
                    if int(time.time()) % 300 == 0:  # Every 5 minutes
                        stats = self.get_cache_stats()
                        self._logger.info(f"Cache stats: {stats}")

                    await asyncio.sleep(1)

                except Exception as e:
                    self._logger.error(f"Error in cache service loop: {e}")
                    await asyncio.sleep(5)

        except KeyboardInterrupt:
            self._logger.info("Cache service interrupted")
        finally:
            await self._shutdown()

    async def _process_event_queue(self):
        """Process queued events"""
        try:
            # Process up to 10 events per iteration to avoid blocking
            for _ in range(10):
                if self._event_queue.empty():
                    break

                event_type, event_data = await self._event_queue.get()
                await self.handle_event(event_type, event_data)
                self._event_queue.task_done()

        except Exception as e:
            self._logger.error(f"Error processing event queue: {e}")

    async def _periodic_cid_caching(self):
        """Periodically cache CIDs from the cids_to_cache_set (equivalent to original cid_cacher)"""
        self._logger.info('Starting periodic CID caching')

        total_processed = 0
        total_cached = 0
        batch_count = 0
        last_summary_time = time.time()
        summary_interval = 60  # Log summary every 60 seconds

        while not self._shutdown_initiated:
            try:
                # Get CIDs that need caching (up to 200 at a time)
                cids_to_cache = await self._redis_conn.spop(cids_to_cache_set(), count=200)
                batch_count += 1

                # Convert bytes to strings if necessary
                cids_to_cache = [cid.decode('utf-8') if isinstance(cid, bytes) else cid for cid in cids_to_cache]

                if cids_to_cache:
                    total_processed += len(cids_to_cache)
                    self._logger.debug(f'Processing batch {batch_count}: {len(cids_to_cache)} CIDs')
                    cached_count = await self._cache_cid_batch(cids_to_cache)
                    if cached_count:
                        total_cached += cached_count
                else:
                    # Periodic summary logging
                    current_time = time.time()
                    if current_time - last_summary_time >= summary_interval:
                        if total_processed > 0:
                            self._logger.info(
                                f'CID cache summary: processed {total_processed} CIDs, '
                                f'cached {total_cached} new in {batch_count} batches '
                                f'(last {summary_interval}s)'
                            )
                        total_processed = 0
                        total_cached = 0
                        batch_count = 0
                        last_summary_time = current_time
                    await asyncio.sleep(60)  # Check every 60 seconds when idle

            except asyncio.CancelledError:
                self._logger.info("Periodic CID caching cancelled")
                break
            except Exception as e:
                self._logger.error(f'Error in periodic CID caching: {e}')
                await asyncio.sleep(60)

    async def _cache_cid_batch(self, cids: List[str]) -> int:
        """Cache a batch of CIDs (equivalent to original cid_cacher._cache_cids)"""
        if not cids:
            return 0

        try:
            # Check which CIDs are already cached
            cid_data = await self._redis_conn.mget([cid_cache(cid) for cid in cids])
            existing_cids = [cid for cid, data in zip(cids, cid_data) if data is not None]
            cids_to_fetch = [cid for cid in cids if cid not in existing_cids]

            # Log skipping if there are significant numbers
            if len(existing_cids) > 0:
                self._logger.debug(f'Skipping {len(existing_cids)} CIDs that are already cached')

            if not cids_to_fetch:
                return 0

            # Fetch data for new CIDs
            tasks = [
                get_submission_data(cid, self._ipfs_reader_client, True)
                for cid in cids_to_fetch
            ]

            results = await asyncio.gather(
                *(asyncio.wait_for(task, timeout=30) for task in tasks),  # 30s timeout
                return_exceptions=True,
            )

            pipeline = self._redis_conn.pipeline()
            batch_cached_count = 0

            for cid, result in zip(cids_to_fetch, results):
                if isinstance(result, Exception):
                    self._logger.debug(f'Error processing CID {cid}: {result}')
                    continue

                snapshot_data = result
                if snapshot_data:
                    # Cache the data
                    cid_cache_key = cid_cache(cid)
                    pipeline.set(cid_cache_key, json.dumps(snapshot_data), ex=PROJECT_DATA_ENTRY_EXPIRY)
                    batch_cached_count += 1

            # Execute the pipeline
            if batch_cached_count > 0:
                await pipeline.execute()
                self._logger.debug(f'Cached {batch_cached_count} new CIDs')

            return batch_cached_count

        except Exception as e:
            self._logger.error(f'Error caching CID batch: {e}')
            return 0

    async def _cleanup_processed_epochs(self):
        """Clean up old processed epochs to prevent memory growth"""
        try:
            # Keep only recent epochs (last 1000) to prevent unbounded growth
            if len(self._processed_epochs) > 1000:
                # Remove oldest entries (this is a simple approximation)
                self._processed_epochs.clear()  # In production, would track timestamps
                self._logger.debug("Cleaned up processed epochs cache")

        except Exception as e:
            self._logger.error(f"Error cleaning up processed epochs: {e}")

    async def queue_event(self, event_type: str, event_data: Dict):
        """Queue an event for processing"""
        try:
            await self._event_queue.put((event_type, event_data))
        except Exception as e:
            self._logger.error(f"Error queuing event {event_type}: {e}")

    async def _shutdown(self):
        """Clean shutdown of the cache service"""
        self._logger.info("Shutting down unified cache service")

        # Cancel all active tasks
        for _, task in self._active_tasks:
            if not task.done():
                task.cancel()

        # Close connections
        if self._aioredis_pool:
            await self._aioredis_pool.close()

        self._logger.info("Unified cache service shutdown complete")

    def stop(self):
        """Stop the cache service"""
        self._shutdown_initiated = True
        self._logger.info("Stop signal received")


# Global cache instance for API access
_cache_instance: Optional[UnifiedCache] = None

async def get_cache_instance() -> UnifiedCache:
    """Get or create the global cache instance"""
    global _cache_instance
    if _cache_instance is None:
        _cache_instance = UnifiedCache("unified-cache")
        # Note: In production, this would be properly initialized
    return _cache_instance

async def get_cached_data(project_id: str, epoch_id: Optional[int] = None) -> Optional[Dict]:
    """Convenience function for getting cached data"""
    cache = await get_cache_instance()
    return await cache.get_cached_data(project_id, epoch_id)

async def queue_cache_event(event_type: str, event_data: Dict):
    """Convenience function for queuing events to the cache service"""
    cache = await get_cache_instance()
    await cache.queue_event(event_type, event_data)


if __name__ == '__main__':
    cache = UnifiedCache('UnifiedCache')
    cache.run()
