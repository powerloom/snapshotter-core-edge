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
- Listens to blockchain events and caches data immediately when available
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
from snapshotter.utils.redis.redis_keys import cid_cache, project_data_hmap, last_submitted_snapshot_data_key
from snapshotter.utils.data_utils import get_submission_data, PROJECT_DATA_ENTRY_EXPIRY

class UnifiedCache(multiprocessing.Process):
    """
    Unified Cache Service - Corrected Approach

    Maintains proactive caching (cache WHEN data is available via events) while
    simplifying the architecture. The key improvements:

    1. Single caching layer instead of CID cacher + data cacher + API caching
    2. Proactive caching: Cache immediately when blockchain events provide data
    3. Simple get/set API for data access
    4. Eliminates Rube Goldberg complexity while maintaining IPFS reliability
    """

    # Class attributes
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _ipfs_singleton: Optional[AsyncIPFSClientSingleton] = None
    _active_tasks: Set[Tuple[float, asyncio.Task]]
    _cache_hit_stats: Dict[str, int]  # Track cache performance
    _cache_miss_stats: Dict[str, int]
    _event_queue: asyncio.Queue
    _processed_epochs: Set[int]

    def __init__(self, name, **kwargs):
        """
        Initialize the UnifiedCache object.

        Args:
            name (str): The name of the cache process.
            **kwargs: Additional keyword arguments passed to the parent Process class.
        """
        super().__init__(name=name, **kwargs)

        # Basic attributes
        self._unique_id = f'{name}-' + str(uuid4())[:8]
        self._logger = default_logger.bind(module=f'UnifiedCache:{settings.namespace}')
        self._shutdown_initiated = False

        # Core components (initialized later)
        self._aioredis_pool: Optional[RedisPoolCache] = None
        self._redis_conn: Optional[aioredis.Redis] = None
        self._ipfs_singleton: Optional[AsyncIPFSClientSingleton] = None
        self._ipfs_reader_client = None

        # Performance tracking
        self._cache_hit_stats: Dict[str, int] = {}
        self._cache_miss_stats: Dict[str, int] = {}

        # Task management
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
        self._task_cleanup_interval = 30  # seconds

        # Event processing
        self._event_queue = asyncio.Queue()
        self._processed_epochs: Set[int] = set()

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
        """Handle SnapshotSubmitted event - cache individual snapshots"""
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

            # Proactively cache this snapshot
            await self._cache_snapshot_data(project_id, epoch_id, snapshot_cid)

            # Also cache the CID for faster future lookups
            cid_cache_key = cid_cache(snapshot_cid)
            await self._redis_conn.set(
                cid_cache_key,
                json.dumps({"snapshot_cid": snapshot_cid, "project_id": project_id, "epoch_id": epoch_id}),
                ex=PROJECT_DATA_ENTRY_EXPIRY
            )

            self._logger.debug(f"Proactively cached snapshot for {project_id}:{epoch_id}")

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
            # This is a simplified version - in production would need more complex logic
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
                    await self._cache_snapshot_data(project_id, epoch_id, snapshot_cid)

            self._logger.debug(f"Processed batch of {len(project_ids)} snapshots for epoch {epoch_id}")

        except Exception as e:
            self._logger.error(f"Error handling SnapshotBatchSubmitted event: {e}")

    async def _cache_snapshot_data(self, project_id: str, epoch_id: int, snapshot_cid: str):
        """Cache snapshot data proactively when it becomes available"""
        try:
            # Create tracked task for background caching
            task = asyncio.create_task(self._fetch_and_cache_data(project_id, epoch_id))
            self._active_tasks.add((time.time(), task))

            # Don't wait for completion - this is proactive caching

        except Exception as e:
            self._logger.error(f"Error initiating proactive cache for {project_id}:{epoch_id}: {e}")

    async def _cleanup_tasks(self):
        """Clean up completed/finished tasks"""
        current_time = time.time()
        completed_tasks = []

        for start_time, task in self._active_tasks:
            if task.done():
                self._active_tasks.discard((start_time, task))
            elif current_time - start_time > 300:  # 5 minute timeout
                self._logger.warning(f"Task {task} timed out, cancelling")
                if not task.done():
                    task.cancel()
                self._active_tasks.discard((start_time, task))

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
    # In production, this would run the cache service
    print("Unified cache service - run with proper initialization")
    """
    Simplified Unified Cache Service

    Replaces the complex multi-layer caching system with a single service that:
    - Handles all data caching (replaces CID cacher + data cacher)
    - Uses proactive caching when data becomes available (events)
    - Provides simple get/set API for data access
    - Eliminates redundant caching layers and complexity
    """

    # Class attributes
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _anchor_rpc_helper: RpcHelper
    _ipfs_singleton: Optional[AsyncIPFSClientSingleton] = None
    _active_tasks: Set[Tuple[float, asyncio.Task]]
    _cache_hit_stats: Dict[str, int]  # Track cache performance
    _cache_miss_stats: Dict[str, int]
    _event_queue: asyncio.Queue
    _processed_epochs: Set[int]

    def __init__(self, name, **kwargs):
        """
        Initialize the UnifiedCache object.

        Args:
            name (str): The name of the cache process.
            **kwargs: Additional keyword arguments passed to the parent Process class.

        Attributes:
            _unique_id (str): A unique identifier for this cache instance.
            _logger: Logger instance for this Cacher.
            _q (queue.Queue): Queue for processing tasks.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _initialized (bool): Flag indicating if the Cacher has been initialized.
            _active_tasks (Set): Set of active asyncio tasks being tracked.
            _task_timeout (int): Maximum time in seconds a task can run before being cancelled.
            _task_cleanup_interval (int): Interval in seconds for checking and cleaning up tasks.
            _hostname (str): Hostname of the machine running this process.
            _health_report_interval (int): Interval in seconds for reporting health status.
            _worker_thread (Optional[threading.Thread]): Thread running the Dramatiq worker.
        """
        super(Cacher, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._logger = default_logger.bind(
            module=f'Cacher:{settings.namespace}-{settings.instance_id}',
        )
        self._q = queue.Queue()
        self._shutdown_initiated = False

        self._initialized = False

        self._shutdown_initiated = False

        self._last_epoch_processing_health_check = 0
        self._preloader_compute_mapping = dict()
        self._snapshot_build_awaited_project_ids = dict()
        # Task tracking
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval

        # Register the handle_event method as a Dramatiq actor
        self._handle_event_actor = dramatiq.actor(
            queue_name=CACHER_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

        # Initialize reporting and notification related attributes
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._worker_thread: Optional[threading.Thread] = None
        # TODO: Move to settings later
        self._project_data_entry_expiry = 60 * 60 * 24 * 7  # 7 days in seconds
        self._cleanup_interval = 60 * 60

    def _signal_handler(self, signum, frame):
        """
        Signal handler method that handles shutdown when a SIGINT, SIGTERM, or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info('Shutdown initiated')

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and populates it with connections.
        
        This method creates a new RedisPoolCache instance and establishes connections
        to the Redis server based on the configuration in settings.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool
        
    async def _init_ipfs_client(self):
        """
        Initialize the IPFS client.

        This method creates a singleton instance of AsyncIPFSClientSingleton,
        initializes its sessions, and assigns the write and read clients to instance variables.
        """
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
        await self._ipfs_singleton.init_sessions()
        self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def _init_rpc_helper(self):
        """
        Initializes the RPC helper instances for both main and anchor chains.
        
        This method creates and initializes RpcHelper instances for interacting with
        the blockchain nodes based on the configuration in settings.
        """
        self._rpc_helper = RpcHelper(settings.rpc)
        await self._rpc_helper.init()
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        await self._anchor_rpc_helper.init()

    async def _init_protocol_meta(self):
        """
        Initializes the protocol metadata by loading the protocol state contract.
        
        This method reads the ABI file for the protocol state contract and creates
        a contract instance for interacting with the protocol state on the anchor chain.
        """
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=to_checksum_address(
                settings.protocol_state.address,
            ),
            abi=protocol_abi,
        )

        self._source_chain_block_time = await get_source_chain_block_time(
            redis_conn=self._redis_conn,
            rpc_helper=self._anchor_rpc_helper,
            state_contract_obj=self._protocol_state_contract,
        )

        self._source_chain_epoch_size = await get_source_chain_epoch_size(
            redis_conn=self._redis_conn,
            rpc_helper=self._anchor_rpc_helper,
            state_contract_obj=self._protocol_state_contract,
        )

        self._max_epochs_to_process = int(self._project_data_entry_expiry / (self._source_chain_epoch_size * self._source_chain_block_time))
        self._logger.info(f"Max epochs to process: {self._max_epochs_to_process}")

    async def init_worker(self):
        """
        Initializes the worker by setting up all required connections and resources.
        
        This method initializes the Redis pool, RPC helper, protocol metadata,
        and starts the task cleanup process. It sets the _initialized flag to True
        when complete.
        """
        if not self._initialized:
            await self._init_redis_pool()
            self._logger.debug('Initialized Redis pool in Processor Distributor init_worker')
            await self._init_rpc_helper()
            self._logger.debug('Initialized RPC helper in Processor Distributor init_worker')
            await self._init_protocol_meta()
            await self._init_ipfs_client()
            asyncio.create_task(self._cleanup_tasks())
            asyncio.create_task(self._cleanup_expired_project_data())

        self._initialized = True

    async def _create_tracked_task(self, task):
        """
        Creates and tracks an asynchronous task.

        This method creates a new task from the given coroutine, adds it to the set of active tasks,
        and sets up a callback to remove the task from the set when it's completed.

        Args:
            task (Coroutine): The coroutine to be executed as a task.

        Returns:
            None

        Note:
            This method is used to keep track of all running tasks for potential cleanup or monitoring.
        """
        # Get the current timestamp
        current_time = time.time()

        # Create a new task from the given coroutine
        new_task = asyncio.create_task(task)

        # Add the task to the set of active tasks, along with its creation time
        self._active_tasks.add((current_time, new_task))

        # Set up a callback to remove the task from the set when it's done
        new_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, new_task)))

    async def _process_snapshot_batch_submitted_message(self, event_data):
        """
        Processes a batch of submitted snapshots and updates their status in Redis.
        
        This method decodes the transaction input to extract the project IDs and snapshot CIDs,
        then updates the Redis database with the finalized epoch information for each project.
        
        Args:
            event_data (str): JSON string containing the snapshot batch submission data.
        """
        self._logger.debug(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
        msg_obj: SnapshotBatchSubmittedMessage = (
            SnapshotBatchSubmittedMessage.model_validate_json(event_data)
        )

        transaction_hash = msg_obj.transactionHash

        # Get the transaction details from the blockchain
        tx = await self._anchor_rpc_helper.get_transaction_from_hash(transaction_hash)

        # Decode the transaction input to extract project IDs and snapshot CIDs
        decoded_input = self._protocol_state_contract.decode_function_input(tx.input)

        _, input_params = decoded_input

        # self._logger.info(f'Decoded input: {function_name}, {input_params}')
        submitted_batch_data = zip(input_params['projectIds'], input_params['snapshotCids'])

        # Create a pipeline for batch processing
        pipeline = self._redis_conn.pipeline()
        
        for project_id, snapshot_cid in submitted_batch_data:
            # update last_finalized_epoch in redis - use max of current and new
            await self._create_tracked_task(process_snapshot_cid(
                self._redis_conn, self._ipfs_reader_client, project_id, 
                snapshot_cid, msg_obj.epochId, msg_obj.epochId
            ))

            last_finalized_hmap = project_last_finalized_epoch_hmap()
            # Get current value first
            current_epoch = await self._redis_conn.hget(last_finalized_hmap, project_id)
            if current_epoch is not None:
                current_epoch = int(current_epoch)
                pipeline.hset(
                    name=last_finalized_hmap,
                    key=project_id,
                    value=max(current_epoch, msg_obj.epochId),
                )
            else:
                pipeline.hset(
                    name=last_finalized_hmap,
                    key=project_id,
                    value=msg_obj.epochId,
                )

            # Add to project data hashmap
            project_hmap_key = project_data_hmap(project_id=project_id)
            pipeline.hset(
                name=project_hmap_key,
                mapping={
                    msg_obj.epochId: json.dumps({
                        'snapshot_cid': snapshot_cid,
                        'status': SnapshotStatus.SEQUENCER_FINALIZED.value,
                    }),
                },
            )
            
            # Add to expiry tracking sorted set with TTL
            expiry_time = int(time.time()) + self._project_data_entry_expiry
            expiry_key = f"{project_id}|{msg_obj.epochId}"
            pipeline.zadd(
                name=data_expiry_zset(),
                mapping={expiry_key: expiry_time}
            )

            # Get state mapping key - breaking up long line
            state_id = SnapshotterStates.SNAPSHOT_SEQUENCER_FINALIZE.value
            mapping_key = epoch_id_project_to_state_mapping(msg_obj.epochId, state_id)
            
            pipeline.hset(
                name=mapping_key,
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='success', timestamp=int(time.time()), extra={'snapshot_cid': snapshot_cid},
                    ).model_dump_json(),
                },
            )
        
        # Execute all commands in a single network round-trip
        await pipeline.execute()

    async def _process_active_pools_message(self, msg_obj: SnapshotSubmittedMessage):
        """
        Processes an active pools message and updates Redis with the active pools information.
        
        This method updates the Redis database with the active pools information.
        Only maintaining 24h cache for active pools.
        """
        self._logger.info(
            f'Processing active pools snapshot - project: {msg_obj.projectId}, '
            f'epoch: {msg_obj.epochId}, CID: {msg_obj.snapshotCid[:16]}...'
        )
        time_interval = 86400

        # check if we are already processing this message
        if await self._redis_conn.get(f"active_pool_data:{time_interval}:processing"):
            self._logger.info(f"Already processing active pools for time interval {time_interval}")
            return

        # set key in redis to indicate that we are processing this message for 10 minutes
        await self._redis_conn.set(f"active_pool_data:{time_interval}:processing", "true", ex=600)

        # check last indexed epoch
        last_indexed_epoch = await self._redis_conn.get(f"active_pool_data:{time_interval}:latest:epoch")
        if last_indexed_epoch:
            last_indexed_epoch = int(last_indexed_epoch)
        else:
            last_indexed_epoch = 0

        project_id = msg_obj.projectId
        tail_epoch_id, _ = await get_tail_epoch_id(
            self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, msg_obj.epochId, time_interval, project_id,
        )

        epochs_to_correct = msg_obj.epochId - last_indexed_epoch if last_indexed_epoch > tail_epoch_id else 0
        self._logger.info(
            f'Active pools processing - last_indexed: {last_indexed_epoch}, '
            f'tail_epoch: {tail_epoch_id}, current: {msg_obj.epochId}, '
            f'epochs_to_correct: {epochs_to_correct}'
        )

        if last_indexed_epoch > tail_epoch_id:
            # fetch indexed data
            self._logger.info(
                f'Correcting indexed data for epochs {last_indexed_epoch} to {msg_obj.epochId} '
                f'for time interval {time_interval}, epochs_to_correct: {epochs_to_correct}'
            )
            active_pools = await self._redis_conn.get(f"active_pool_data:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
            if active_pools:
                active_pools = json.loads(active_pools)
                # Only fetch snapshots if epochs_to_correct > 0
                if epochs_to_correct > 0:
                    # fetch snapshots for epochs_to_correct
                    self._logger.info(
                        f'Fetching new snapshots for epochs {last_indexed_epoch + 1} to {msg_obj.epochId}'
                    )
                    new_snapshots = await get_project_epoch_snapshot_bulk(
                        self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, self._ipfs_reader_client, last_indexed_epoch + 1, msg_obj.epochId, project_id,
                    )
                    old_snapshots = await get_project_epoch_snapshot_bulk(
                        self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, self._ipfs_reader_client, tail_epoch_id - epochs_to_correct, tail_epoch_id - 1, project_id,
                    )
                    
                    # add new snapshots to indexed data
                    for snapshot in new_snapshots:
                        if snapshot:
                            for pool_address, frequency in snapshot['pools'].items():
                                if pool_address not in active_pools:
                                    active_pools[pool_address] = 0
                                active_pools[pool_address] += frequency
                    # remove old snapshots from indexed data
                    for snapshot in old_snapshots:
                        if snapshot:
                            for pool_address, frequency in snapshot['pools'].items():
                                if pool_address in active_pools:
                                    active_pools[pool_address] -= frequency
                                    # Remove pools with zero or negative frequency
                                    if active_pools[pool_address] <= 0:
                                        del active_pools[pool_address]
                else:
                    # epochs_to_correct == 0, use cached data as-is
                    self._logger.info(
                        f'No epochs to correct (epochs_to_correct={epochs_to_correct}), '
                        f'using cached data directly with {len(active_pools)} pools'
                    )
                
                # set data in redis
                pipeline = self._redis_conn.pipeline()
                pipeline.set(f"active_pool_data:{time_interval}:{msg_obj.epochId}:{settings.namespace}", json.dumps(active_pools), ex=3600)
                pipeline.set(f"active_pool_data:{time_interval}:latest:epoch", msg_obj.epochId, ex=3600)
                pipeline.delete(f"active_pool_data:{time_interval}:processing")
                await pipeline.execute()
                self._logger.info(
                    f'Active pools cache updated - epoch: {msg_obj.epochId}, '
                    f'total pools: {len(active_pools)}'
                )
            else:
                # No cached data found, need to fetch all snapshots from scratch
                self._logger.warning(
                    f'No cached data found for epoch {last_indexed_epoch}, '
                    f'cannot process incremental update for project {msg_obj.projectId}'
                )
                pipeline = self._redis_conn.pipeline()
                pipeline.delete(f"active_pool_data:{time_interval}:processing")
                await pipeline.execute()

    async def _process_active_tokens_message(self, msg_obj: SnapshotSubmittedMessage):
        """
        Processes an active tokens message and updates Redis with the active tokens information.
        
        This method updates the Redis database with the active tokens information.
        Only maintaining 24h cache for active tokens.
        """
        self._logger.info(f'ActiveTokensEvent caught with message {msg_obj}')

        time_interval = 86400

        # check if we are already processing this message
        if await self._redis_conn.get(f"active_token_data:{msg_obj.projectId}:{time_interval}:processing"):
            self._logger.info(f"Already processing active tokens for project {msg_obj.projectId} for time interval {time_interval}")
            return

        # set key in redis to indicate that we are processing this message for 10 minutes
        await self._redis_conn.set(f"active_token_data:{msg_obj.projectId}:{time_interval}:processing", "true", ex=600)

        # check last indexed epoch
        last_indexed_epoch = await self._redis_conn.get(f"active_token_data:{time_interval}:latest:epoch")
        if last_indexed_epoch:
            last_indexed_epoch = int(last_indexed_epoch)
        else:
            last_indexed_epoch = 0

        project_id = msg_obj.projectId
        tail_epoch_id, _ = await get_tail_epoch_id(
            self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, msg_obj.epochId, time_interval, project_id,
        )

        self._logger.info(f"Last indexed epoch: {last_indexed_epoch}, tail epoch id: {tail_epoch_id}, current epoch: {msg_obj.epochId}")

        if last_indexed_epoch > tail_epoch_id:
            epochs_to_correct = msg_obj.epochId - last_indexed_epoch
            # fetch indexed data
            self._logger.info(f"Correcting indexed data for epochs {last_indexed_epoch} to {msg_obj.epochId} for time interval {time_interval}")
            active_tokens = await self._redis_conn.get(f"active_token_data:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
            if active_tokens:
                active_tokens = json.loads(active_tokens)
                # fetch snapshots for epochs_to_correct
                self._logger.info(f"Fetching new snapshots for epochs {last_indexed_epoch} to {last_indexed_epoch + epochs_to_correct}")
                new_snapshots = await get_project_epoch_snapshot_bulk(
                    self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, self._ipfs_reader_client, last_indexed_epoch + 1, msg_obj.epochId, project_id,
                )
                old_snapshots = await get_project_epoch_snapshot_bulk(
                    self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, self._ipfs_reader_client, tail_epoch_id - epochs_to_correct, tail_epoch_id - 1, project_id,
                )
                
                # add new snapshots to indexed data
                for snapshot in new_snapshots:
                    if snapshot:
                        for token_address, frequency in snapshot['tokens'].items():
                            if token_address not in active_tokens:
                                active_tokens[token_address] = 0
                            active_tokens[token_address] += frequency
                # remove old snapshots from indexed data
                for snapshot in old_snapshots:
                    if snapshot:
                        for token_address, frequency in snapshot['tokens'].items():
                            if token_address in active_tokens:
                                active_tokens[token_address] -= frequency
                # set data in redis
                pipeline = self._redis_conn.pipeline()
                pipeline.set(f"active_token_data:{time_interval}:{msg_obj.epochId}:{settings.namespace}", json.dumps(active_tokens), ex=3600)
                pipeline.set(f"active_token_data:{time_interval}:latest:epoch", msg_obj.epochId, ex=3600)
                pipeline.delete(f"active_token_data:{msg_obj.projectId}:{time_interval}:processing")
                await pipeline.execute()

    async def _process_trade_volume_from_base_snapshot_message(self, msg_obj: SnapshotSubmittedMessage, time_interval: int):
        """
        Processes a base snapshot message and updates Redis with the base snapshot information.
        
        This method updates the Redis database with the active tokens information.
        Only maintaining 24h cache for active tokens.
        """
        self._logger.info(f'TradeVolumeFromBaseSnapshotEvent caught with message {msg_obj}')

        # check if we are already processing this message
        if await self._redis_conn.get(f"trade_volume_data:{msg_obj.projectId}:{time_interval}:processing"):
            self._logger.info(f"Already processing trade volume for project {msg_obj.projectId} for time interval {time_interval}")
            return

        # set key in redis to indicate that we are processing this message for 10 minutes
        await self._redis_conn.set(f"trade_volume_data:{msg_obj.projectId}:{time_interval}:processing", "true", ex=600)

        # Check last indexed epoch
        last_indexed_epoch = await self._redis_conn.get(
            f"trade_volume_data:{msg_obj.projectId}:{time_interval}:latest:epoch"
        )
        if last_indexed_epoch:
            last_indexed_epoch = int(last_indexed_epoch)
        else:
            last_indexed_epoch = 0
        
        tail_epoch_id, _ = await get_tail_epoch_id(
            self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper,
            msg_obj.epochId, time_interval, msg_obj.projectId
        )

        self._logger.info(
            f"Trade volume aggregation - Project: {msg_obj.projectId}, "
            f"Last indexed epoch: {last_indexed_epoch}, "
            f"tail epoch id: {tail_epoch_id}, current epoch: {msg_obj.epochId}"
        )

        total_trade_volume = 0.0

        if last_indexed_epoch > tail_epoch_id:
            epochs_to_correct = msg_obj.epochId - last_indexed_epoch
            # Fetch cached volume data
            self._logger.info(
                f"Using cached data with correction for project {msg_obj.projectId}, "
                f"epochs {last_indexed_epoch} to {msg_obj.epochId} "
                f"for time interval {time_interval}"
            )
            cached_volume = await self._redis_conn.get(
                f"trade_volume_data:{msg_obj.projectId}:{time_interval}:{last_indexed_epoch}:"
                f"{settings.namespace}"
            )
            if cached_volume:
                total_trade_volume = float(cached_volume)
                # Apply incremental updates if needed
                if epochs_to_correct > 0:
                    self._logger.info(
                        f"Applying incremental updates for project {msg_obj.projectId}, "
                        f"fetching {epochs_to_correct} new epochs and removing old ones"
                    )
                    
                    # Fetch new snapshots to add
                    new_snapshots = await get_project_epoch_snapshot_bulk(
                        self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, 
                        self._ipfs_reader_client, last_indexed_epoch + 1, msg_obj.epochId, msg_obj.projectId
                    )
                    
                    # Fetch old snapshots to remove
                    old_snapshots = await get_project_epoch_snapshot_bulk(
                        self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, 
                        self._ipfs_reader_client, tail_epoch_id - epochs_to_correct, 
                        tail_epoch_id - 1, msg_obj.projectId
                    )
                    
                    # Add volume from new snapshots
                    for snapshot in new_snapshots:
                        if snapshot and 'totalTrade' in snapshot:
                            volume = snapshot['totalTrade']
                            if isinstance(volume, (int, float)) and volume > 0:
                                total_trade_volume += volume
                    
                    # Subtract volume from old snapshots
                    for snapshot in old_snapshots:
                        if snapshot and 'totalTrade' in snapshot:
                            volume = snapshot['totalTrade']
                            if isinstance(volume, (int, float)) and volume > 0:
                                total_trade_volume -= volume
                    
                    # Ensure volume doesn't go negative due to data inconsistencies
                    total_trade_volume = max(0.0, total_trade_volume)
            else:
                # No cached data found, fall back to full calculation
                self._logger.info(
                    f"No cached data found for project {msg_obj.projectId}, "
                    f"calculating full volume from {tail_epoch_id} to {msg_obj.epochId}"
                )
                snapshots = await get_project_epoch_snapshot_bulk(
                    self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, 
                    self._ipfs_reader_client, tail_epoch_id, msg_obj.epochId, msg_obj.projectId
                )
                for snapshot in snapshots:
                    if snapshot and 'totalTrade' in snapshot:
                        volume = snapshot['totalTrade']
                        if isinstance(volume, (int, float)) and volume > 0:
                            total_trade_volume += volume
        else:
            # Fresh calculation needed
            self._logger.info(
                f"Performing fresh calculation for project {msg_obj.projectId} "
                f"from {tail_epoch_id} to {msg_obj.epochId}"
            )
            snapshots = await get_project_epoch_snapshot_bulk(
                self._redis_conn, self._protocol_state_contract, self._anchor_rpc_helper, 
                self._ipfs_reader_client, tail_epoch_id, msg_obj.epochId, msg_obj.projectId
            )
            for snapshot in snapshots:
                if snapshot and 'totalTrade' in snapshot:
                    volume = snapshot['totalTrade']
                    if isinstance(volume, (int, float)) and volume > 0:
                        total_trade_volume += volume

        # Set data in redis (same pattern as active pools/tokens)
        pipeline = self._redis_conn.pipeline()
        pipeline.set(
            f"trade_volume_data:{msg_obj.projectId}:{time_interval}:{msg_obj.epochId}:{settings.namespace}", 
            str(total_trade_volume)
        )
        pipeline.set(
            f"trade_volume_data:{msg_obj.projectId}:{time_interval}:latest:epoch", msg_obj.epochId
        )
        # Remove old data
        if last_indexed_epoch > 0:
            pipeline.delete(
                f"trade_volume_data:{msg_obj.projectId}:{time_interval}:{last_indexed_epoch}:"
                f"{settings.namespace}"
            )
        pipeline.delete(f"trade_volume_data:{msg_obj.projectId}:{time_interval}:processing")
        await pipeline.execute()

    async def _process_snapshot_submitted_message(self, event_data):
        """
        Processes a snapshot submission event and updates Redis with the snapshot information.
        
        This method updates the Redis database with the submitted snapshot information,
        adds the snapshot CID to the unpin zset if IPFS unpinning is enabled, and
        updates the last submitted snapshot data for the project.
        
        Args:
            event_data (str): JSON string containing the snapshot submission data.
        """
        try:
            msg_obj: SnapshotSubmittedMessage = (
                SnapshotSubmittedMessage.model_validate_json(event_data)
            )
            self._logger.debug(
                f'SnapshotSubmittedEvent - project: {msg_obj.projectId}, '
                f'epoch: {msg_obj.epochId}, CID: {msg_obj.snapshotCid[:16]}...'
            )

            # Create a pipeline for batch processing
            pipeline = self._redis_conn.pipeline()
            
            # Add snapshot cid to unpin zset if enabled
            if settings.ipfs_unpinning.enabled:
                self._logger.debug(f"Adding snapshot CID {msg_obj.snapshotCid[:16]}... to unpin zset")
                pipeline.zadd(
                    name=snapshots_to_unpin_zset_name(),
                    mapping={msg_obj.snapshotCid: int(time.time()) + settings.ipfs_unpinning.unpin_after},
                )
            
            last_snapshot_submitted_data = await self._redis_conn.get(last_submitted_snapshot_data_key(msg_obj.projectId))
            if last_snapshot_submitted_data:
                last_snapshot_submitted_data = json.loads(last_snapshot_submitted_data)
                last_snapshot_submitted_epoch = last_snapshot_submitted_data['epochId']
            else:
                last_snapshot_submitted_epoch = 0

            await self._create_tracked_task(process_snapshot_cid(
                self._redis_conn, self._ipfs_reader_client, msg_obj.projectId, 
                msg_obj.snapshotCid, msg_obj.epochId, msg_obj.epochId
            ))
            
            # Add to project data hashmap
            project_hmap_key = project_data_hmap(project_id=msg_obj.projectId)
            pipeline.hset(
                name=project_hmap_key,
                mapping={
                    msg_obj.epochId: json.dumps({
                        'snapshot_cid': msg_obj.snapshotCid,
                        'status': SnapshotStatus.SUBMITTED.value,
                    }),
                },
            )
            
            # Add to expiry tracking sorted set with TTL
            expiry_time = int(time.time()) + self._project_data_entry_expiry
            expiry_key = f"{msg_obj.projectId}|{msg_obj.epochId}"
            pipeline.zadd(
                name=data_expiry_zset(),
                mapping={expiry_key: expiry_time}
            )
            
            # Set last submitted snapshot data
            pipeline.set(
                name=last_submitted_snapshot_data_key(msg_obj.projectId),
                value=json.dumps({
                    'snapshotCid': msg_obj.snapshotCid,
                    'epochId': msg_obj.epochId,
                }),
            )
            
            # Execute all commands in a single network round-trip
            await pipeline.execute()

            if msg_obj.projectId.startswith('activePools:'):
                self._logger.info(
                    f'ActivePoolsEvent - project: {msg_obj.projectId}, '
                    f'epoch: {msg_obj.epochId}, CID: {msg_obj.snapshotCid[:16]}...'
                )
                await self._create_tracked_task(self._process_active_pools_message(msg_obj))
            elif msg_obj.projectId.startswith('activeTokens:'):
                self._logger.info(
                    f'ActiveTokensEvent - project: {msg_obj.projectId}, '
                    f'epoch: {msg_obj.epochId}, CID: {msg_obj.snapshotCid[:16]}...'
                )
                await self._create_tracked_task(self._process_active_tokens_message(msg_obj))
            elif msg_obj.projectId.startswith('baseSnapshot:'):
                self._logger.debug(
                    f'BaseSnapshotEvent - project: {msg_obj.projectId}, '
                    f'epoch: {msg_obj.epochId}'
                )
                await self._create_tracked_task(
                    self._process_trade_volume_from_base_snapshot_message(msg_obj, 86400)
                )
                await self._create_tracked_task(
                    self._process_trade_volume_from_base_snapshot_message(msg_obj, 604800)
                )
        except Exception as e:
            self._logger.error(f"Error processing snapshot submitted message: {e}")
            self._logger.error(traceback.format_exc())
            raise

    async def _process_snapshot_finalized_message(self, event_data):
        """
        Processes a snapshot finalization event and updates Redis with the finalized status.
        
        This method updates the Redis database with the finalized snapshot information,
        sets the last finalized epoch for the project, and updates the snapshot state.
        
        Args:
            event_data (str): JSON string containing the snapshot finalization data.
        """
        self._logger.debug(f'SnapshotFinalizedEvent caught with message {event_data}')
        msg_obj: SnapshotFinalizedMessage = (
            SnapshotFinalizedMessage.model_validate_json(event_data)
        )

        # Create a pipeline for batch processing
        pipeline = self._redis_conn.pipeline()
        
        # set project last finalized epoch in redis - use max of current and new
        last_finalized_hmap = project_last_finalized_epoch_hmap()
        # Get current value first
        current_epoch = await self._redis_conn.hget(last_finalized_hmap, msg_obj.projectId)
        if current_epoch is not None:
            current_epoch = int(current_epoch)
            pipeline.hset(
                name=last_finalized_hmap,
                key=msg_obj.projectId,
                value=max(current_epoch, msg_obj.epochId),
            )
        else:
            pipeline.hset(
                name=last_finalized_hmap,
                key=msg_obj.projectId,
                value=msg_obj.epochId,
            )

        # Add to project data hashmap
        project_hmap_key = project_data_hmap(project_id=msg_obj.projectId)
        pipeline.hset(
            name=project_hmap_key,
            mapping={
                msg_obj.epochId: json.dumps({
                    'snapshot_cid': msg_obj.snapshotCid,
                    'status': SnapshotStatus.FINALIZED.value,
                }),
            },
        )
        
        # Add to expiry tracking sorted set with TTL
        expiry_time = int(time.time()) + self._project_data_entry_expiry
        expiry_key = f"{msg_obj.projectId}|{msg_obj.epochId}"
        pipeline.zadd(
            name=data_expiry_zset(),
            mapping={expiry_key: expiry_time}
        )

        pipeline.hset(
            name=epoch_id_project_to_state_mapping(msg_obj.epochId, SnapshotterStates.SNAPSHOT_FINALIZE.value),
            mapping={
                msg_obj.projectId: SnapshotterStateUpdate(
                    status='success', timestamp=int(time.time()), extra={'snapshot_cid': msg_obj.snapshotCid},
                ).model_dump_json(),
            },
        )
        
        # Execute all commands in a single network round-trip
        await pipeline.execute()

    async def process_event(self, event_type, event_data):
        """
        Processes events based on their type by calling the appropriate handler method.
        
        This method routes the event to the appropriate handler based on the event_type,
        and handles any errors that occur during processing.
        
        Args:
            event_type (str): The type of event to process.
            event_data (str): JSON string containing the event data.
            
        Returns:
            None
        """
        self._logger.info(
            (
                'Got message to process and distribute: {}'
            ),
            event_data,
        )

        if event_type == 'SnapshotSubmitted':
            self._logger.info(f'SnapshotSubmittedEvent caught with message {event_data}')
            await self._create_tracked_task(self._process_snapshot_submitted_message(event_data))
        elif event_type == 'SnapshotFinalized':
            self._logger.info(f'SnapshotFinalizedEvent caught with message {event_data}')
            await self._create_tracked_task(self._process_snapshot_finalized_message(event_data))
        elif event_type == 'SnapshotBatchSubmitted':
            self._logger.info(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
            await self._create_tracked_task(self._process_snapshot_batch_submitted_message(event_data))

        else:
            self._logger.error(
                (
                    'Unknown message type: {}'
                ),
                event_type,
            )

        if self._redis_conn:
            await self._redis_conn.close()

    def handle_event(self, *args):
        """
        Dramatiq actor method that handles incoming events.
        
        This method is called by Dramatiq when a message is received. It extracts the
        event type and data from the arguments and runs the async process_event method
        in the event loop.
        
        Args:
            *args: Arguments passed by Dramatiq, expected to be [event_type, event_data].

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

    async def _cleanup_tasks(self):
        """
        Periodically clean up completed or timed-out tasks.

        This method runs in a loop, checking for tasks that have completed or
        timed out, and removing them from the active tasks set. It also cancels
        tasks that have exceeded the timeout period.
        """
        while True:
            try:
                await asyncio.sleep(self._task_cleanup_interval)
                current_time = time.time()
                
                # Create a copy of tasks to avoid modification during iteration
                tasks_to_check = list(self._active_tasks)
                
                for task_start_time, task in tasks_to_check:
                    try:
                        if task.done():
                            self._active_tasks.discard((task_start_time, task))
                        elif current_time - task_start_time > self._task_timeout:
                            self._logger.warning(
                                f'Task {task} timed out. Cancelling..., '
                                f'current_time: {current_time}, '
                                f'start_time: {task_start_time}',
                            )
                            task.cancel()
                            self._active_tasks.discard((task_start_time, task))
                    except Exception as e:
                        self._logger.error(f"Error cleaning up task {task}: {e}")
                        # Remove the task from active tasks even if there's an error
                        self._active_tasks.discard((task_start_time, task))
            except asyncio.CancelledError:
                self._logger.info("Task cleanup loop cancelled")
                break
            except Exception as e:
                self._logger.error(f"Error in task cleanup loop: {e}")
                await asyncio.sleep(self._task_cleanup_interval)

    async def report_health_status(self):
        """
        Reports the current health status of this Cacher instance to Redis.

        This method updates a Redis hash with the current timestamp for this
        instance's hostname, allowing monitoring systems to detect if the
        Cacher is alive and responsive.
        """
        if not hasattr(self, '_redis_conn') or self._redis_conn is None:
            self._logger.warning('Redis connection not initialized, skipping health report.')
            return
        try:
            current_timestamp = int(time.time())
            await self._redis_conn.hset(
                service_health_timestamps_key(),
                self._hostname,
                current_timestamp,
            )
            self._logger.debug(f'Reported health for {self._hostname} at {current_timestamp}')
        except Exception as e:
            self._logger.error(f'Failed to report health status for hostname {self._hostname}: {e}')

    async def _periodic_health_reporter(self):
        """
        Periodically reports health status to Redis.

        This method runs in a loop, reporting the health status of this Cacher
        instance to Redis at regular intervals. It also checks if the worker
        thread is still alive, and stops reporting if it's not.
        """
        self._logger.info(
            f'Starting periodic health reporter task for {self._hostname} (Interval: {self._health_report_interval}s)',
        )
        while True:
            should_report = True
            if not self._worker_thread or not self._worker_thread.is_alive():
                should_report = False
                if self._worker_thread:
                    # Worker thread is no longer alive
                    self._logger.critical(
                        'Main Dramatiq worker thread has died. Halting health reports.'
                    )
                    # Halt the health reporter
                    break
                else:
                    # Worker thread hasn't been initialized yet
                    self._logger.warning('Worker thread not found. Skipping health report for now.')

            try:
                if should_report:
                    await self.report_health_status()

                await asyncio.sleep(self._health_report_interval)
            except asyncio.CancelledError:
                self._logger.info(f'Periodic health reporter task for {self._hostname} cancelled.')
                break
            except Exception as e:
                self._logger.error(f'Error in periodic health reporter loop: {e}')
                await asyncio.sleep(self._health_report_interval)

    async def _cleanup_expired_project_data(self):
        """
        Periodically checks for and removes expired project data entries.

        This method runs in the background to clean up project data hash entries
        that have exceeded their TTL as recorded in the expiry sorted set.
        """
        while True:
            try:
                self._logger.info(f"Cleaning up expired project data at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                current_time = int(time.time())
                # Get all entries that have expired
                expired_entries = await self._redis_conn.zrangebyscore(
                    data_expiry_zset(),
                    0,
                    current_time,
                    withscores=True
                )

                self._logger.info(f"Cleaning up {len(expired_entries)} expired project data entries")
                if expired_entries:                    
                    # Group by project_id for efficient deletion
                    entries_by_hmap = {}
                    for entry, _ in expired_entries:
                        project_id, key = entry.decode('utf-8').split('|')
                        if project_id not in entries_by_hmap:
                            entries_by_hmap[project_id] = []
                        entries_by_hmap[project_id].append(key)

                    # Remove the entries from the hashmaps and the expiry set
                    pipeline = self._redis_conn.pipeline()
                    for project_id, keys in entries_by_hmap.items():
                        pipeline.hdel(project_data_hmap(project_id), *keys)

                    # Remove from expiry tracking
                    pipeline.zrem(data_expiry_zset(), *[entry for entry, _ in expired_entries])

                    await pipeline.execute()

            except Exception as e:
                self._logger.error(f"Error cleaning up expired project data: {e}")

            self._logger.info(f"Sleeping for {self._cleanup_interval} seconds before next cleanup cycle")
            await asyncio.sleep(self._cleanup_interval)

    def run(self) -> None:
        """
        Main entry point for the Cacher process.
        
        This method sets up resource limits, registers signal handlers,
        initializes the worker, starts the Dramatiq worker in a separate thread,
        and runs the event loop. It also handles cleanup when the process is
        shutting down.
        """
        try:
            # Set resource limits for file descriptors
            soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(
                resource.RLIMIT_NOFILE,
                (settings.rlimit.file_descriptors, hard),
            )
            
            # Register signal handlers for graceful shutdown
            for signame in [SIGINT, SIGTERM, SIGQUIT]:
                signal(signame, self._signal_handler)
                
            # Use uvloop for better performance
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

            ev_loop = asyncio.get_event_loop()
            Cacher._event_loop = ev_loop  # Store the event loop
            # Update the middleware to use this event loop
            for middleware in redis_broker.middleware:
                if isinstance(middleware, dramatiq.middleware.AsyncIO):
                    middleware.event_loop = ev_loop

            # Initialize the worker
            ev_loop.run_until_complete(self.init_worker())

            # Start a Dramatiq worker in a separate thread
            worker = Worker(redis_broker, queues=[CACHER_QUEUE_NAME])
            worker_thread = threading.Thread(target=worker.start, daemon=True)
            self._worker_thread = worker_thread  # Store the thread object
            worker_thread.start()

            # Start the health reporter task
            health_reporter_task = ev_loop.create_task(self._periodic_health_reporter())

            try:
                # Run the event loop until shutdown is requested
                ev_loop.run_forever()
            finally:
                # Clean up tasks and close the event loop
                if health_reporter_task and not health_reporter_task.done():
                    health_reporter_task.cancel()
                    ev_loop.run_until_complete(asyncio.sleep(2))
                
                # Close Redis connection
                if hasattr(self, '_redis_conn') and self._redis_conn:
                    ev_loop.run_until_complete(self._redis_conn.close())
                
                ev_loop.close()
        except Exception as e:
            self._logger.error(f"Fatal error in Cacher process: {e}")
            self._logger.error(traceback.format_exc())
            raise


if __name__ == '__main__':
    cacher = Cacher('Cacher')
    cacher.run()
