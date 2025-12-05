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
import queue
import resource
import threading
import time
import traceback
import os
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from uuid import uuid4
from ipfs_client.main import AsyncIPFSClientSingleton
from ipfs_client.dag import IPFSAsyncClientError
from redis import asyncio as aioredis

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker
from eth_utils.address import to_checksum_address
from eth_utils.crypto import keccak
from rpc_helper.rpc import RpcHelper
from signal import SIGINT, SIGTERM, SIGQUIT
from signal import signal
from socket import gethostname

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotStatus, SnapshotterStates, SnapshotterStateUpdate
from snapshotter.utils.models.message_models import SnapshotBatchSubmittedMessage
from snapshotter.utils.models.message_models import SnapshotFinalizedMessage
from snapshotter.utils.models.message_models import SnapshotSubmittedMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import (
    cid_cache, project_data_hmap, last_submitted_snapshot_data_key, 
    data_expiry_zset, project_last_finalized_epoch_hmap, cids_to_cache_set,
    snapshots_to_unpin_zset_name, service_health_timestamps_key,
    epoch_id_project_to_state_mapping
)
from snapshotter.utils.dramatiq_queues import CACHER_QUEUE_NAME
from snapshotter.utils.data_utils import (
    get_submission_data, PROJECT_DATA_ENTRY_EXPIRY, get_project_config,
    get_tail_epoch_id, get_project_epoch_snapshot_bulk, process_snapshot_cid,
    get_source_chain_block_time, get_source_chain_epoch_size
)

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
    Unified Cache Service - Replaces cacher + cid_cacher

    Handles all snapshot caching functionality:
    - Event-driven snapshot processing
    - CID batch caching
    - Health reporting
    - Data cleanup
    """

    # Class attributes (same as original cacher)
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _anchor_rpc_helper: RpcHelper
    _snapshot_build_awaited_project_ids: Dict[int, Set[str]]
    _slot_id_to_snapshotters: Dict[int, Dict[str, str]]
    _slot_id_to_timeslot: Dict[int, int]
    _registered_slots: List[int]
    _last_synced_slot_info: int
    _source_chain_epoch_size: int
    _source_chain_id: int
    _event_loop = None  # Class variable to store the event loop
    _active_tasks: Set[Tuple[float, asyncio.Task]]

    def __init__(self, name, **kwargs):
        super(UnifiedCache, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._logger = default_logger.bind(
            module=f'UnifiedCache:{settings.namespace}-{settings.instance_id}',
        )
        self._q = queue.Queue()
        self._shutdown_initiated = False
        self._initialized = False

        self._last_epoch_processing_health_check = 0
        self._preloader_compute_mapping = dict()
        self._snapshot_build_awaited_project_ids = dict()

        # Task tracking (same as original cacher)
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
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
        self._project_data_entry_expiry = 60 * 60 * 24 * 7  # 7 days in seconds
        self._cleanup_interval = 60 * 60
        self._caching_interval = 60
        self._cid_cache_expiry = PROJECT_DATA_ENTRY_EXPIRY

    def _signal_handler(self, signum, frame):
        """Signal handler for graceful shutdown"""
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info('Shutdown initiated')

    async def _init_redis_pool(self):
        """Initialize Redis connection pool"""
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_ipfs_client(self):
        """Initialize IPFS client"""
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
        await self._ipfs_singleton.init_sessions()
        self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def _init_rpc_helper(self):
        """Initialize RPC helpers"""
        self._rpc_helper = RpcHelper(settings.rpc)
        await self._rpc_helper.init()
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        await self._anchor_rpc_helper.init()

    async def _init_protocol_meta(self):
        """Initialize protocol metadata"""
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=to_checksum_address(settings.protocol_state.address),
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
        """Initialize worker (same as original cacher)"""
        if not self._initialized:
            await self._init_redis_pool()
            self._logger.debug('Initialized Redis pool')
            await self._init_rpc_helper()
            self._logger.debug('Initialized RPC helper')
            await self._init_protocol_meta()
            await self._init_ipfs_client()
            asyncio.create_task(self._cleanup_tasks())
            asyncio.create_task(self._cleanup_expired_project_data())

        self._initialized = True

    async def _create_tracked_task(self, task):
        """Create and track an async task (same as original cacher)"""
        current_time = time.time()
        new_task = asyncio.create_task(task)
        self._active_tasks.add((current_time, new_task))
        new_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, new_task)))

    async def _get_project_epoch_snapshot_bulk_locked(
        self, project_id: str, epoch_id_min: int, epoch_id_max: int
    ):
        """
        Wrapper around get_project_epoch_snapshot_bulk with per-project locking
        to prevent concurrent bulk fetches for the same project.
        """
        lock_key = f"bulk_fetch_processing:{project_id}"
        
        # Try to acquire lock atomically (SET with NX - only set if not exists)
        lock_acquired = await self._redis_conn.set(lock_key, "true", ex=600, nx=True)
        
        if not lock_acquired:
            # Lock already held, wait for it to be released
            self._logger.debug(
                f"Bulk fetch already in progress for project {project_id}, "
                f"waiting for completion (epochs {epoch_id_min} to {epoch_id_max})"
            )
            # Wait for lock to be released (with timeout)
            wait_start = time.time()
            while await self._redis_conn.exists(lock_key):
                if time.time() - wait_start > 300:  # 5 minute timeout
                    self._logger.warning(
                        f"Timeout waiting for bulk fetch lock for project {project_id}"
                    )
                    return []
                await asyncio.sleep(1)
            
            # Try to acquire lock again after waiting
            lock_acquired = await self._redis_conn.set(lock_key, "true", ex=600, nx=True)
            if not lock_acquired:
                # Still couldn't acquire, another process got it
                self._logger.debug(
                    f"Could not acquire bulk fetch lock for project {project_id} after waiting"
                )
                return []
        
        try:
            # Perform the bulk fetch
            return await get_project_epoch_snapshot_bulk(
                self._redis_conn,
                self._protocol_state_contract,
                self._anchor_rpc_helper,
                self._ipfs_reader_client,
                epoch_id_min,
                epoch_id_max,
                project_id,
            )
        finally:
            # Release lock
            await self._redis_conn.delete(lock_key)

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

    async def process_event(self, event_type, event_data):
        """Process events - directly await handlers, inner tasks handle their own tracking"""
        self._logger.info(
            (
                'Got message to process and distribute: {}'
            ),
            event_data,
        )

        if event_type == 'SnapshotSubmitted':
            self._logger.info(f'SnapshotSubmittedEvent caught with message {event_data}')
            await self._process_snapshot_submitted_message(event_data)
        elif event_type == 'SnapshotFinalized':
            self._logger.info(f'SnapshotFinalizedEvent caught with message {event_data}')
            await self._process_snapshot_finalized_message(event_data)
        elif event_type == 'SnapshotBatchSubmitted':
            self._logger.info(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
            await self._process_snapshot_batch_submitted_message(event_data)
        else:
            self._logger.error(
                (
                    'Unknown message type: {}'
                ),
                event_type,
            )

    async def _process_snapshot_submitted_message(self, event_data):
        """Process snapshot submitted message (same as original cacher)"""
        try:
            msg_obj: SnapshotSubmittedMessage = SnapshotSubmittedMessage.model_validate_json(event_data)
            self._logger.debug(
                f'SnapshotSubmittedEvent - project: {msg_obj.projectId}, '
                f'epoch: {msg_obj.epochId}, CID: {msg_obj.snapshotCid[:16]}...'
            )

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
        """Process snapshot finalized message (same as original cacher)"""
        self._logger.debug(f'SnapshotFinalizedEvent caught with message {event_data}')
        msg_obj: SnapshotFinalizedMessage = SnapshotFinalizedMessage.model_validate_json(event_data)

        pipeline = self._redis_conn.pipeline()

        # set project last finalized epoch in redis - use max of current and new
        last_finalized_hmap = project_last_finalized_epoch_hmap()
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

        await pipeline.execute()

    async def _process_snapshot_batch_submitted_message(self, event_data):
        """Process snapshot batch submitted message (same as original cacher)"""
        self._logger.debug(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
        msg_obj: SnapshotBatchSubmittedMessage = SnapshotBatchSubmittedMessage.model_validate_json(event_data)

        transaction_hash = msg_obj.transactionHash

        # Get the transaction details from the blockchain
        tx = await self._anchor_rpc_helper.get_transaction_from_hash(transaction_hash)

        # Decode the transaction input to extract project IDs and snapshot CIDs
        decoded_input = self._protocol_state_contract.decode_function_input(tx.input)
        _, input_params = decoded_input

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

            # Get state mapping key
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
        """Process active pools message (same as original cacher)"""
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
                    new_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                        project_id, last_indexed_epoch + 1, msg_obj.epochId
                    )
                    old_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                        project_id, tail_epoch_id - epochs_to_correct, tail_epoch_id - 1
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
        """Process active tokens message (same as original cacher)"""
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
                new_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                    project_id, last_indexed_epoch + 1, msg_obj.epochId
                )
                old_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                    project_id, tail_epoch_id - epochs_to_correct, tail_epoch_id - 1
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
        """Process trade volume from base snapshot message (same as original cacher)"""
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
                    new_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                        msg_obj.projectId, last_indexed_epoch + 1, msg_obj.epochId
                    )

                    # Fetch old snapshots to remove
                    old_snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                        msg_obj.projectId, tail_epoch_id - epochs_to_correct, tail_epoch_id - 1
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
                snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                    msg_obj.projectId, tail_epoch_id, msg_obj.epochId
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
            snapshots = await self._get_project_epoch_snapshot_bulk_locked(
                msg_obj.projectId, tail_epoch_id, msg_obj.epochId
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
            pipeline.delete(f"trade_volume_data:{msg_obj.projectId}:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
        pipeline.delete(f"trade_volume_data:{msg_obj.projectId}:{time_interval}:processing")
        await pipeline.execute()

    async def report_health_status(self):
        """Report health status to Redis (equivalent to original cacher)"""
        if not hasattr(self, '_redis_conn') or self._redis_conn is None:
            self._logger.warning('Redis connection not initialized, skipping health report.')
            return

        try:
            from snapshotter.utils.redis.redis_keys import service_health_timestamps_key
            current_timestamp = int(time.time())
            hostname = f"{os.getpid()}-{settings.instance_id}"
            await self._redis_conn.hset(
                service_health_timestamps_key(),
                hostname,
                current_timestamp,
            )
            self._logger.debug(f'Reported health for {hostname} at {current_timestamp}')
        except Exception as e:
            self._logger.error(f'Failed to report health status: {e}')

    async def _periodic_health_reporter(self):
        """Periodically report health status (equivalent to original cacher)"""
        health_report_interval = getattr(settings, 'health_report_interval', 30)

        while not self._shutdown_initiated:
            try:
                # Check if worker thread is still alive
                if hasattr(self, '_worker_thread') and self._worker_thread and not self._worker_thread.is_alive():
                    self._logger.critical('Dramatiq worker thread has died. Halting health reports.')
                    break

                await self.report_health_status()
                await asyncio.sleep(health_report_interval)

            except asyncio.CancelledError:
                self._logger.info('Periodic health reporter task cancelled.')
                break
            except Exception as e:
                self._logger.error(f'Error in periodic health reporter loop: {e}')
                await asyncio.sleep(health_report_interval)

    async def _cleanup_expired_project_data(self):
        """Clean up expired project data (equivalent to original cacher)"""
        while not self._shutdown_initiated:
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
                        entry_str = entry.decode('utf-8') if isinstance(entry, bytes) else entry
                        project_id, key = entry_str.split('|')
                        if project_id not in entries_by_hmap:
                            entries_by_hmap[project_id] = []
                        entries_by_hmap[project_id].append(key)

                    # Remove the entries from the hashmaps and the expiry set
                    pipeline = self._redis_conn.pipeline()
                    for project_id, keys in entries_by_hmap.items():
                        from snapshotter.utils.redis.redis_keys import project_data_hmap
                        pipeline.hdel(project_data_hmap(project_id), *keys)

                    # Remove from expiry tracking
                    pipeline.zrem(data_expiry_zset(), *[entry for entry, _ in expired_entries])

                    await pipeline.execute()

            except Exception as e:
                self._logger.error(f"Error cleaning up expired project data: {e}")

            self._logger.info(f"Sleeping for {self._cleanup_interval} seconds before next cleanup cycle")
            await asyncio.sleep(self._cleanup_interval)

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

            # Fetch snapshot data from IPFS (checks Redis cache first)
            snapshot_data = await get_submission_data(
                snapshot_cid, 
                self._ipfs_reader_client, 
                False,
                redis_conn=self._redis_conn,
                project_id=project_id,
            )

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
            ev_loop.run_until_complete(self.init_worker())

            # Start a Dramatiq worker in a separate thread
            worker = Worker(redis_broker, queues=[CACHER_QUEUE_NAME])
            worker_thread = threading.Thread(target=worker.start, daemon=True)
            self._worker_thread = worker_thread
            worker_thread.start()

            # Start periodic CID caching task (from cid_cacher)
            cid_cache_task = ev_loop.create_task(self._periodic_cid_caching())

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

                if cid_cache_task and not cid_cache_task.done():
                    cid_cache_task.cancel()
                    ev_loop.run_until_complete(asyncio.sleep(2))

                # Close Redis connection
                if hasattr(self, '_redis_conn') and self._redis_conn:
                    ev_loop.run_until_complete(self._redis_conn.close())

                ev_loop.close()

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
                    cached_count = await self._cache_cids(cids_to_cache)
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
                    await asyncio.sleep(self._caching_interval)

            except asyncio.CancelledError:
                self._logger.info("Periodic CID caching cancelled")
                break
            except Exception as e:
                self._logger.error(f'Error in periodic CID caching: {e}')
                await asyncio.sleep(self._caching_interval)

    async def _cache_cids(self, cids: List[str]):
        """Cache CIDs in Redis (same as original cid_cacher)"""
        if not cids:
            return

        try:
            # fetch existing cids cached from redis
            cid_data = await self._redis_conn.mget(
                [cid_cache(cid) for cid in cids]
            )
            existing_cids = [cid for cid, data in zip(cids, cid_data) if data is not None]
            cids_to_fetch = [cid for cid in cids if cid not in existing_cids]

            # Only log skipping if there are significant numbers, use DEBUG for routine operations
            if len(existing_cids) > 0:
                self._logger.debug(f'Skipping {len(existing_cids)} CIDs that are already cached (fetching {len(cids_to_fetch)} new)')

            if not cids_to_fetch:
                self._logger.debug(f'All {len(cids)} CIDs already cached, skipping fetch')
                return 0

            tasks = [
                get_submission_data(cid, self._ipfs_reader_client, True)
                for cid in cids_to_fetch
            ]

            results = await asyncio.gather(
                *(asyncio.wait_for(task, timeout=10) for task in tasks),
                return_exceptions=True,
            )

            pipeline = self._redis_conn.pipeline()
            batch_cached_count = 0
            error_count = 0

            for cid, result in zip(cids_to_fetch, results):
                if isinstance(result, Exception):
                    self._logger.debug(f'Error processing CID {cid}: {result}')
                    error_count += 1
                    continue

                snapshot_data = result
                if snapshot_data:
                    # cache lite snapshot in redis
                    cid_cache_key = cid_cache(cid)
                    pipeline.set(
                        name=cid_cache_key,
                        value=json.dumps(snapshot_data),
                        ex=self._cid_cache_expiry,
                    )
                    batch_cached_count += 1
                else:
                    self._logger.warning(f'No snapshot data found for CID: {cid}')

            if batch_cached_count > 0:
                await pipeline.execute()

            # Log summary with context
            if batch_cached_count > 0:
                self._logger.info(
                    f'CID cache update: cached {batch_cached_count} new CIDs '
                    f'(skipped {len(existing_cids)} already cached'
                    f'{f", {error_count} errors" if error_count > 0 else ""})'
                )
            elif len(cids_to_fetch) > 0:
                self._logger.warning(
                    f'CID cache update: failed to cache any of {len(cids_to_fetch)} CIDs'
                    f'{f" ({error_count} errors)" if error_count > 0 else ""}'
                )

            return batch_cached_count

        except Exception as e:
            self._logger.error(f'Error caching CIDs: {e}')
            raise

    async def _cleanup_tasks(self):
        """Periodically clean up completed or timed-out tasks (same as original cacher)"""
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
        """Report health status to Redis (same as original cacher)"""
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
        """Periodically report health status to Redis (same as original cacher)"""
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
        """Periodically clean up expired project data (same as original cacher)"""
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
