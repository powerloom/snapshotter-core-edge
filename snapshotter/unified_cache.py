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
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from uuid import uuid4
from ipfs_client.main import AsyncIPFSClientSingleton
from ipfs_client.dag import IPFSAsyncClientError
from redis import asyncio as aioredis

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import cid_cache, project_data_hmap, last_submitted_snapshot_data_key, data_expiry_zset, project_last_finalized_epoch_hmap, cids_to_cache_set
from snapshotter.utils.dramatiq_queues import CACHER_QUEUE_NAME
from snapshotter.utils.rpc import RpcHelper
from snapshotter.utils.file_utils import read_json_file
from eth_utils.address import to_checksum_address
from snapshotter.utils.data_utils import get_submission_data, PROJECT_DATA_ENTRY_EXPIRY, get_project_config

# Configure Redis broker for Dramatiq (same as original cacher)
redis_broker = RedisBroker(host=settings.redis.host, port=settings.redis.port, db=settings.redis.db)
redis_broker.add_middleware(AsyncIO())

# Remove Prometheus middleware to avoid errors (same as original)
middleware = redis_broker.middleware[:]  # Make a copy
for m in middleware:
    if m.__class__.__name__ == 'Prometheus':
        redis_broker.middleware.remove(m)

dramatiq.set_broker(redis_broker)


class UnifiedCache(multiprocessing.Process):
    """
    Simplified Unified Cache Service

    Replaces the complex multi-layer caching system with a single service that:
    - Handles all data caching (replaces CID cacher + data cacher)
    - Uses cache-on-demand strategy with background refresh
    - Provides simple get/set API for data access
    - Eliminates redundant caching layers and complexity
    """

    # Class variable to store the event loop (same as original cacher)
    _event_loop = None

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

        # RPC and contract components (for transaction decoding)
        self._rpc_helper: Optional[RpcHelper] = None
        self._anchor_rpc_helper: Optional[RpcHelper] = None
        self._protocol_state_contract = None

        # Cache performance tracking
        self._cache_hit_stats: Dict[str, int] = {}
        self._cache_miss_stats: Dict[str, int] = {}

        # Active tasks for background processing
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
        self._task_cleanup_interval = 30  # seconds

        # Event processing
        self._event_queue = asyncio.Queue()
        self._processed_epochs: Set[int] = set()

        # Dramatiq worker thread (same as original cacher)
        self._worker_thread: Optional[threading.Thread] = None

        # Register the handle_event method as a Dramatiq actor (same as original cacher)
        self._handle_event_actor = dramatiq.actor(
            queue_name=CACHER_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

    def _signal_handler(self, signum, frame):
        """
        Signal handler method that handles shutdown when a SIGINT, SIGTERM, or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        import signal
        if signum in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info(f'Shutdown initiated by signal {signum}')

            # Cancel all active tasks gracefully
            if hasattr(self, '_event_loop') and self._event_loop:
                self._event_loop.call_soon_threadsafe(self._event_loop.stop)  # Track processed epochs to avoid duplicates

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

        # RPC helpers (for transaction decoding like original cacher)
        self._rpc_helper = RpcHelper(settings.rpc)
        await self._rpc_helper.init()

        self._anchor_rpc_helper = RpcHelper(settings.anchor_chain_rpc)
        await self._anchor_rpc_helper.init()

        # Protocol state contract (for transaction decoding like original cacher)
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=to_checksum_address(settings.protocol_state.address),
            abi=protocol_abi
        )

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

    def handle_event(self, *args):
        """
        Dramatiq actor method that handles incoming events.

        This method is called by Dramatiq when a message is received. It extracts the
        event type and data from the arguments and runs the async process_event method
        in the event loop.

        Args:
            *args: Arguments passed by Dramatiq, expected to be [event_type, event_data].
                  event_data can be a JSON string or dict.

        Returns:
            None
        """
        try:
            self._logger.warning(f'Handling event: {args}')
            event_type = args[0]
            event_data = args[1]

            # Run the async process_event in the event loop
            future = asyncio.run_coroutine_threadsafe(
                self.process_event(event_type, event_data),
                self._event_loop,
            )
            # Wait for the result with timeout
            future.result(timeout=60)

            self._logger.debug(f'Event has been handled: {args}')

            return None
        except Exception as e:
            # Capture the full traceback for better debugging
            error_traceback = ''.join(
                traceback.format_exception(type(e), e, e.__traceback__),
            )
            self._logger.error(f'Error processing event: {e}')
            self._logger.error(f'Detailed traceback:\n{error_traceback}')
            self._logger.error(f'Event data: {args}')

    async def process_event(self, event_type: str, event_data: Union[str, Dict[str, Any]]) -> None:
        """
        Processes events based on their type by calling the appropriate handler method.

        This method routes the event to the appropriate handler based on the event_type,
        and handles any errors that occur during processing.

        Args:
            event_type (str): The type of event to process.
            event_data (Union[str, Dict[str, Any]]): JSON string or dict containing the event data.
                  Will be parsed to dict if string.

        Returns:
            None
        """
        # Parse JSON string to dictionary if needed
        if isinstance(event_data, str):
            try:
                parsed_event_data = json.loads(event_data)
            except json.JSONDecodeError as e:
                self._logger.error(f'Failed to parse event data JSON: {e}')
                self._logger.error(f'Raw event data: {event_data}')
                return
        elif isinstance(event_data, dict):
            parsed_event_data = event_data
        else:
            self._logger.error(f'Invalid event_data type: {type(event_data)}, expected str or dict')
            return

        # Validate parsed data is a dict
        if not isinstance(parsed_event_data, dict):
            self._logger.error(f'Parsed event data is not a dict: {type(parsed_event_data)}')
            return

        self._logger.info(f'Got message to process: {parsed_event_data}')

        if event_type == 'SnapshotSubmitted':
            self._logger.info('SnapshotSubmittedEvent caught')
            await self._handle_snapshot_submitted(parsed_event_data)
        elif event_type == 'SnapshotFinalized':
            self._logger.info('SnapshotFinalizedEvent caught')
            await self._handle_snapshot_finalized(parsed_event_data)
        elif event_type == 'SnapshotBatchSubmitted':
            self._logger.info('SnapshotBatchSubmittedEvent caught')
            await self._handle_snapshot_batch_submitted(parsed_event_data)
        else:
            self._logger.error(f'Unknown message type: {event_type}')

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

            # Create a pipeline for batch processing (like original)
            pipeline = self._redis_conn.pipeline()

            # Add snapshot cid to unpin zset if IPFS unpinning is configured
            try:
                if hasattr(settings, 'ipfs_unpinning') and settings.ipfs_unpinning.enabled:
                    from snapshotter.utils.redis.redis_keys import snapshots_to_unpin_zset_name
                    self._logger.debug(f"Adding snapshot CID {snapshot_cid[:16]}... to unpin zset")
                    pipeline.zadd(
                        name=snapshots_to_unpin_zset_name(),
                        mapping={snapshot_cid: int(time.time()) + settings.ipfs_unpinning.unpin_after},
                    )
            except AttributeError:
                # IPFS unpinning not configured, skip
                pass

            # Update last submitted snapshot data (like original)
            from snapshotter.utils.redis.redis_keys import last_submitted_snapshot_data_key
            last_snapshot_submitted_data = await self._redis_conn.get(last_submitted_snapshot_data_key(project_id))
            if last_snapshot_submitted_data:
                last_snapshot_submitted_data = json.loads(last_snapshot_submitted_data)
                last_snapshot_submitted_epoch = last_snapshot_submitted_data['epochId']
            else:
                last_snapshot_submitted_epoch = 0

            # Process snapshot CID equivalent to original process_snapshot_cid
            await self._process_snapshot_cid(project_id, snapshot_cid, epoch_id, epoch_id)

            # Update project hashmap with this snapshot (like original)
            project_hmap_key = project_data_hmap(project_id=project_id)
            pipeline.hset(
                project_hmap_key,
                str(epoch_id),
                json.dumps({
                    'snapshot_cid': snapshot_cid,
                    'status': 'SUBMITTED'
                })
            )

            # Execute pipeline
            await pipeline.execute()

            self._logger.debug(f"Processed snapshot for {project_id}:{epoch_id} (CID: {snapshot_cid[:16]}...)")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotSubmitted event: {e}")

    async def _handle_snapshot_finalized(self, event_data: Dict):
        """Handle SnapshotFinalized event - equivalent to original cacher logic"""
        try:
            epoch_id = event_data.get("epochId")
            project_id = event_data.get("projectId")
            snapshot_cid = event_data.get("snapshotCid")

            if not all([epoch_id, project_id, snapshot_cid]):
                self._logger.warning(f"Incomplete SnapshotFinalized event data: {event_data}")
                return

            # Create a pipeline for batch processing (like original)
            pipeline = self._redis_conn.pipeline()

            # Update project last finalized epoch - use max of current and new (like original)
            last_finalized_hmap = project_last_finalized_epoch_hmap()
            current_epoch = await self._redis_conn.hget(last_finalized_hmap, project_id)
            if current_epoch is not None:
                current_epoch = int(current_epoch)
                pipeline.hset(
                    last_finalized_hmap,
                    project_id,
                    max(current_epoch, epoch_id),
                )
            else:
                pipeline.hset(
                    last_finalized_hmap,
                    project_id,
                    epoch_id,
                )

            # Update project data hashmap (like original)
            project_hmap_key = project_data_hmap(project_id=project_id)
            pipeline.hset(
                project_hmap_key,
                str(epoch_id),
                json.dumps({
                    'snapshot_cid': snapshot_cid,
                    'status': 'FINALIZED'
                })
            )

            # Add to expiry tracking sorted set with TTL (like original)
            from snapshotter.utils.redis.redis_keys import data_expiry_zset
            expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY
            pipeline.zadd(data_expiry_zset, {f"{project_id}|{epoch_id}": expiry_time})

            # Execute pipeline
            await pipeline.execute()

            self._logger.debug(f"Marked epoch {epoch_id} as finalized for {project_id}")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotFinalized event: {e}")

    async def _handle_snapshot_batch_submitted(self, event_data: Dict):
        """Handle SnapshotBatchSubmitted event - equivalent to original cacher logic with transaction decoding"""
        try:
            transaction_hash = event_data.get("transactionHash")
            epoch_id = event_data.get("epochId")

            if not all([transaction_hash, epoch_id]):
                self._logger.warning(f"Incomplete SnapshotBatchSubmitted event data: {event_data}")
                return

            # Decode transaction to get project IDs and snapshot CIDs (like original cacher)
            tx = await self._anchor_rpc_helper.get_transaction_from_hash(transaction_hash)
            decoded_input = self._protocol_state_contract.decode_function_input(tx.input)
            _, input_params = decoded_input

            project_ids = input_params['projectIds']
            snapshot_cids = input_params['snapshotCids']

            self._logger.debug(f"Decoded batch transaction: {len(project_ids)} projects, epoch {epoch_id}")

            # Create a pipeline for batch processing (like original)
            pipeline = self._redis_conn.pipeline()

            # Process each snapshot in the batch (like original)
            for project_id, snapshot_cid in zip(project_ids, snapshot_cids):
                epoch_key = f"{project_id}:{epoch_id}"
                if epoch_key not in self._processed_epochs:
                    self._processed_epochs.add(epoch_key)

                    # Process snapshot CID (equivalent to original process_snapshot_cid)
                    await self._process_snapshot_cid(project_id, snapshot_cid, epoch_id, epoch_id)

                    # Update last finalized epoch - use max of current and new (like original)
                    from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_hmap
                    last_finalized_hmap = project_last_finalized_epoch_hmap()
                    current_epoch = await self._redis_conn.hget(last_finalized_hmap, project_id)
                    if current_epoch is not None:
                        current_epoch = int(current_epoch)
                        pipeline.hset(
                            last_finalized_hmap,
                            project_id,
                            max(current_epoch, epoch_id),
                        )
                    else:
                        pipeline.hset(
                            last_finalized_hmap,
                            project_id,
                            epoch_id,
                        )

                    # Update project data hashmap (like original)
                    project_hmap_key = project_data_hmap(project_id=project_id)
                    pipeline.hset(
                        project_hmap_key,
                        str(epoch_id),
                        json.dumps({
                            'snapshot_cid': snapshot_cid,
                            'status': 'FINALIZED'  # Batch submissions are typically finalized
                        })
                    )

                    # Add to expiry tracking (like original)
                    from snapshotter.utils.redis.redis_keys import data_expiry_zset
                    expiry_time = int(time.time()) + PROJECT_DATA_ENTRY_EXPIRY
                    pipeline.zadd(data_expiry_zset, {f"{project_id}|{epoch_id}": expiry_time})

            # Execute pipeline
            await pipeline.execute()

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

    def run(self):
        """
        Main entry point for the UnifiedCache process.

        This method sets up resource limits, registers signal handlers,
        initializes the worker, starts the Dramatiq worker in a separate thread,
        and runs the event loop. It also handles cleanup when the process is
        shutting down.
        """
        try:
            # Set resource limits for file descriptors (same as original)
            import resource
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(
                resource.RLIMIT_NOFILE,
                (settings.rlimit.file_descriptors, hard),
            )
            self._logger.info(f'Set file descriptor limit to {settings.rlimit.file_descriptors}')

            # Register signal handlers for graceful shutdown
            import signal
            for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
                signal.signal(signame, self._signal_handler)

            # Use uvloop for better performance
            import uvloop
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

            ev_loop = asyncio.get_event_loop()
            UnifiedCache._event_loop = ev_loop  # Store the event loop
            self._event_loop = ev_loop

            # Set event loop for AsyncIO middleware
            for middleware in redis_broker.middleware:
                if isinstance(middleware, AsyncIO):
                    middleware.event_loop = ev_loop

            # Initialize the worker
            ev_loop.run_until_complete(self._init_components())

            # Start a Dramatiq worker in a separate thread (same as original cacher)
            worker = Worker(redis_broker, queues=[CACHER_QUEUE_NAME])
            worker_thread = threading.Thread(target=worker.start, daemon=True)
            self._worker_thread = worker_thread
            worker_thread.start()

            # Start periodic CID caching task (equivalent to original cid_cacher)
            cid_cache_task = ev_loop.create_task(self._periodic_cid_caching())
            self._active_tasks.add((time.time(), cid_cache_task))

            try:
                self._logger.info('UnifiedCache started successfully')
                # Run the event loop until shutdown is requested
                ev_loop.run_forever()
            except KeyboardInterrupt:
                self._logger.info('KeyboardInterrupt received, shutting down')
            finally:
                self._logger.info('Stopping event loop and cleaning up')

                # Cancel periodic tasks
                if not cid_cache_task.done():
                    cid_cache_task.cancel()

                # Perform graceful shutdown
                ev_loop.run_until_complete(self._shutdown())

                # Close the event loop
                ev_loop.close()
                self._logger.info('UnifiedCache shutdown complete')

        except Exception as e:
            self._logger.error(f"Fatal error in UnifiedCache process: {e}")
            self._logger.error(traceback.format_exc())
            raise

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
                *(asyncio.wait_for(task, timeout=10) for task in tasks),  # 10s timeout like original
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
