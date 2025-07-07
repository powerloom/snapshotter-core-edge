import json
import asyncio
import multiprocessing
import queue
import resource
import threading
import time
import traceback
from rpc_helper.rpc import RpcHelper
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from socket import gethostname
from typing import Dict
from typing import List
from typing import Set
from typing import Optional
from typing import Tuple
from uuid import uuid4
from ipfs_client.main import AsyncIPFSClientSingleton

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker
from eth_utils.address import to_checksum_address
from eth_utils.crypto import keccak
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.data_models import SnapshotStatus
from snapshotter.utils.models.message_models import SnapshotBatchSubmittedMessage
from snapshotter.utils.models.message_models import SnapshotFinalizedMessage
from snapshotter.utils.models.message_models import SnapshotSubmittedMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import project_data_hmap
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_hmap
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key
from snapshotter.utils.redis.redis_keys import snapshots_to_unpin_zset_name
from snapshotter.utils.redis.redis_keys import last_submitted_snapshot_data_key
from snapshotter.utils.redis.redis_keys import data_expiry_zset
from snapshotter.utils.dramatiq_queues import CACHER_QUEUE_NAME
from snapshotter.utils.data_utils import get_source_chain_block_time
from snapshotter.utils.data_utils import get_source_chain_epoch_size
from snapshotter.utils.data_utils import get_tail_epoch_id
from snapshotter.utils.data_utils import get_project_epoch_snapshot_bulk
from snapshotter.utils.data_utils import process_snapshot_cid

# Configure Redis broker with no middleware
redis_broker = RedisBroker(host=settings.redis.host, port=settings.redis.port, db=settings.redis.db)
redis_broker.add_middleware(AsyncIO())

# Remove Prometheus middleware to avoid errors
middleware = redis_broker.middleware[:]  # Make a copy
for m in middleware:
    if m.__class__.__name__ == 'Prometheus':
        redis_broker.middleware.remove(m)

# redis_broker.middleware.clear()  # Remove ALL middlewares
dramatiq.set_broker(redis_broker)


class Cacher(multiprocessing.Process):
    """
    A class responsible for distributing processing tasks and managing the snapshot lifecycle.

    This class handles epoch releases, project updates, snapshot submissions, and aggregations.
    It interacts with Dramatiq for message passing and Redis for state management.
    
    The Cacher is responsible for:
    1. Processing snapshot submission events
    2. Updating Redis with snapshot status information
    3. Managing the lifecycle of snapshots from submission to finalization
    4. Reporting health status to Redis
    """

    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _anchor_rpc_helper: RpcHelper
    _snapshot_build_awaited_project_ids: Dict[int, Set[str]]  # epoch_id: project_ids
    _slot_id_to_snapshotters: Dict[int, Dict[str, str]]  # slot_id: {snapshotters}
    _slot_id_to_timeslot: Dict[int, int]  # slot_id: timeslot
    _registered_slots: List[int]
    _last_synced_slot_info: int
    _source_chain_epoch_size: int
    _source_chain_id: int
    _event_loop = None  # Class variable to store the event loop
    _active_tasks: Set[Tuple[float, asyncio.Task]]  # Set of (start_time, task) tuples

    def __init__(self, name, **kwargs):
        """
        Initialize the Cacher object.

        Args:
            name (str): The name of the Cacher process.
            **kwargs: Additional keyword arguments passed to the parent Process class.

        Attributes:
            _unique_id (str): A unique identifier for this Cacher instance.
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
        self._logger.debug(f'ActivePoolsEvent caught with message {msg_obj}')
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

        self._logger.info(f"Last indexed epoch: {last_indexed_epoch}, tail epoch id: {tail_epoch_id}, current epoch: {msg_obj.epochId}")

        if last_indexed_epoch > tail_epoch_id:
            epochs_to_correct = msg_obj.epochId - last_indexed_epoch
            # fetch indexed data
            self._logger.info(f"Correcting indexed data for epochs {last_indexed_epoch} to {msg_obj.epochId} for time interval {time_interval}")
            active_pools = await self._redis_conn.get(f"active_pool_data:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
            if active_pools:
                active_pools = json.loads(active_pools)
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
                # set data in redis
                pipeline = self._redis_conn.pipeline()
                pipeline.set(f"active_pool_data:{time_interval}:{msg_obj.epochId}:{settings.namespace}", json.dumps(active_pools))
                pipeline.set(f"active_pool_data:{time_interval}:latest:epoch", msg_obj.epochId)
                # remove old data
                if last_indexed_epoch > 0:
                    pipeline.delete(f"active_pool_data:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
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
                pipeline.set(f"active_token_data:{time_interval}:{msg_obj.epochId}:{settings.namespace}", json.dumps(active_tokens))
                pipeline.set(f"active_token_data:{time_interval}:latest:epoch", msg_obj.epochId)
                # remove old data
                if last_indexed_epoch > 0:
                    pipeline.delete(f"active_token_data:{time_interval}:{last_indexed_epoch}:{settings.namespace}")
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
            self._logger.info(f'SnapshotSubmittedEvent caught with message {event_data}')
            msg_obj: SnapshotSubmittedMessage = (
                SnapshotSubmittedMessage.model_validate_json(event_data)
            )

            # Create a pipeline for batch processing
            pipeline = self._redis_conn.pipeline()
            
            # Add snapshot cid to unpin zset if enabled
            if settings.ipfs_unpinning.enabled:
                self._logger.info("Adding snapshot cid to unpin zset")
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
                self._logger.info(f'ActivePoolsEvent caught with message, sending it to active pools processor {msg_obj}')
                await self._create_tracked_task(self._process_active_pools_message(msg_obj))
            elif msg_obj.projectId.startswith('activeTokens:'):
                await self._create_tracked_task(self._process_active_tokens_message(msg_obj))
            elif msg_obj.projectId.startswith('baseSnapshot:'):
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
