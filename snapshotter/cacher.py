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
from uuid import uuid4

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
from snapshotter.utils.models.message_models import SnapshotBatchSubmittedMessage
from snapshotter.utils.models.message_models import SnapshotFinalizedMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import project_finalized_data_zset
from snapshotter.utils.redis.redis_keys import project_last_finalized_epoch_key
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key

# Configure Redis broker with no middleware
redis_broker = RedisBroker(host=settings.redis.host, port=settings.redis.port)
redis_broker.add_middleware(AsyncIO())

# Remove Prometheus middleware to avoid errors
middleware = redis_broker.middleware[:]  # Make a copy
for m in middleware:
    if m.__class__.__name__ == 'Prometheus':
        redis_broker.middleware.remove(m)

# redis_broker.middleware.clear()  # Remove ALL middlewares
dramatiq.set_broker(redis_broker)

CACHER_QUEUE_NAME = f'powerloom-cacher_{settings.namespace}_{settings.instance_id}'


class Cacher(multiprocessing.Process):
    """
    A class responsible for distributing processing tasks and managing the snapshot lifecycle.

    This class handles epoch releases, project updates, snapshot submissions, and aggregations.
    It interacts with Dramatiq for message passing and Redis for state management.
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

    def __init__(self, name, **kwargs):
        """
        Initialize the ProcessorDistributor object.

        Args:
            name (str): The name of the ProcessorDistributor.
            **kwargs: Additional keyword arguments.

        Attributes:
            _unique_id (str): The unique ID of the ProcessorDistributor.
            _q (queue.Queue): The queue used for processing tasks.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _rpc_helper: The RPC helper object.
            _source_chain_id: The source chain ID.
            _projects_list: The list of projects.
            _initialized (bool): Flag indicating if the ProcessorDistributor has been initialized.
            _callback_exchange_name (str): The name of the exchange for callbacks.
            _payload_commit_exchange_name (str): The name of the exchange for payload commits.
            _payload_commit_routing_key (str): The routing key for payload commits.
            _upcoming_project_changes (defaultdict): Dictionary of upcoming project changes.
            _preload_completion_conditions (defaultdict): Dictionary of preload completion conditions.
            _shutdown_initiated (bool): Flag indicating if shutdown has been initiated.
            _all_preload_tasks (set): Set of all preload tasks.
            _project_type_config_mapping (dict): Dictionary mapping project types to their configurations.
            _last_epoch_processing_health_check (int): Timestamp of the last epoch processing health check.
            _preloader_compute_mapping (dict): Dictionary mapping preloader tasks to compute resources.
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

        self._handle_event_actor = dramatiq.actor(
            queue_name=CACHER_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

        # Initialize reporting and notification related attributes

        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._worker_thread: Optional[threading.Thread] = None

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
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _init_rpc_helper(self):
        """
        Initializes the RpcHelper instance if it is not already initialized.
        """
        self._rpc_helper = RpcHelper(settings.rpc)
        await self._rpc_helper.init()
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        await self._anchor_rpc_helper.init()

    async def _init_protocol_meta(self):
        """
        Initializes the protocol metadata by fetching the source chain epoch size and source chain ID.
        """
        protocol_abi = read_json_file(settings.protocol_state.abi, self._logger)
        self._protocol_state_contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=to_checksum_address(
                settings.protocol_state.address,
            ),
            abi=protocol_abi,
        )

    async def init_worker(self):
        """
        Initializes the worker by initializing the Redis pool, RPC helper, loading project metadata,
        initializing the preloader compute mapping.
        """
        if not self._initialized:
            await self._init_redis_pool()
            self._logger.debug('Initialized Redis pool in Processor Distributor init_worker')
            await self._init_rpc_helper()
            self._logger.debug('Initialized RPC helper in Processor Distributor init_worker')
            await self._init_protocol_meta()
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True

    # NOTE: Considering SequencerFinalized state as Finalized for now
    # data data is overwritten upon receiving SnapshotFinalized message for the project
    # TODO: Create separate states for SequencerFinalized and SnapshotFinalized
    async def _cache_submitted_snapshot(self, event_data):
        """
        Caches the snapshot data and forwards it to the payload commit queue.

        Args:
            message (IncomingMessage): The incoming message containing the snapshot data.

        Returns:
            None
        """
        self._logger.debug(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
        msg_obj: SnapshotBatchSubmittedMessage = (
            SnapshotBatchSubmittedMessage.parse_raw(event_data)
        )

        transaction_hash = msg_obj.transactionHash

        tx = await self._anchor_rpc_helper.get_transaction_from_hash(transaction_hash)

        decoded_input = self._protocol_state_contract.decode_function_input(tx.input)

        _, input_params = decoded_input

        # self._logger.info(f'Decoded input: {function_name}, {input_params}')
        submitted_batch_data = zip(input_params['projectIds'], input_params['snapshotCids'])

        for project_id, snapshot_cid in submitted_batch_data:
            # update last_finalized_epoch in redis
            await self._redis_conn.set(
                name=project_last_finalized_epoch_key(project_id),
                value=msg_obj.epochId,
                ex=60,
            )

            # Add to project finalized data zset
            await self._redis_conn.zadd(
                project_finalized_data_zset(project_id=project_id),
                {snapshot_cid: msg_obj.epochId},
            )

            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(msg_obj.epochId, SnapshotterStates.SNAPSHOT_FINALIZE.value),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='success', timestamp=int(time.time()), extra={'snapshot_cid': snapshot_cid},
                    ).model_dump_json(),
                },
            )

    async def _cache_finalized_snapshot(self, event_data):
        """
        Caches the snapshot data and forwards it to the payload commit queue.

        Args:
            message (IncomingMessage): The incoming message containing the snapshot data.

        Returns:
            None
        """
        self._logger.debug(f'SnapshotFinalizedEvent caught with message {event_data}')
        msg_obj: SnapshotFinalizedMessage = (
            SnapshotFinalizedMessage.parse_raw(event_data)
        )

        # set project last finalized epoch in redis
        await self._redis_conn.set(
            name=project_last_finalized_epoch_key(msg_obj.projectId),
            value=msg_obj.epochId,
            ex=60,
        )

        # Add to project finalized data zset
        await self._redis_conn.zadd(
            project_finalized_data_zset(project_id=msg_obj.projectId),
            {msg_obj.snapshotCid: msg_obj.epochId},
        )

        await self._redis_conn.hset(
            name=epoch_id_project_to_state_mapping(msg_obj.epochId, SnapshotterStates.SNAPSHOT_FINALIZE.value),
            mapping={
                msg_obj.projectId: SnapshotterStateUpdate(
                    status='success', timestamp=int(time.time()), extra={'snapshot_cid': msg_obj.snapshotCid},
                ).model_dump_json(),
            },
        )

        self._logger.trace(f'Payload Commit Message Distribution time - {int(time.time())}')

    async def process_event(self, event_type, event_data):
        """
        Callback function to handle incoming Dramatiq messages.

        Args:
            message (IncomingMessage): The incoming Dramatiq message.

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
            pass
        elif event_type == 'SnapshotFinalized':
            self._logger.info(f'SnapshotFinalizedEvent caught with message {event_data}')
            await self._cache_finalized_snapshot(
                event_data,
            )

        elif event_type == 'SnapshotBatchSubmitted':
            self._logger.info(f'SnapshotBatchSubmittedEvent caught with message {event_data}')
            await self._cache_submitted_snapshot(
                event_data,
            )

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
        Handle event without being an async function directly.
        This allows Dramatiq to call it normally while still using your async code.
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
            # Wait for the result
            future.result()  # 60 second timeout

            self._logger.warning(f'Event has been handled: {args}')

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
        """
        while True:
            await asyncio.sleep(self._task_cleanup_interval)
            for task_start_time, task in list(self._active_tasks):
                current_time = time.time()
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

    async def report_health_status(self):
        """Reports the current timestamp for this container's hostname to Redis."""
        if not hasattr(self, '_redis_conn') or self._redis_conn is None:
            self._logger.warning('Redis connection not initialized, skipping health report.')
            return
        try:
            current_timestamp = int(time.time())
            await self._redis_conn.hset(
                service_health_timestamps_key,
                self._hostname,
                current_timestamp,
            )
            self._logger.debug(f'Reported health for {self._hostname} at {current_timestamp}')
        except Exception as e:
            self._logger.error(f'Failed to report health status for hostname {self._hostname}: {e}')

    async def _periodic_health_reporter(self):
        """Periodically reports health status."""
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

    def run(self) -> None:
        """
        Runs the ProcessorDistributor by setting resource limits, registering signal handlers,
        initializing the worker, starting the Dramatiq worker, and running the event loop.
        """
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.get_event_loop()
        Cacher._event_loop = ev_loop  # Store the event loop
        # Update the middleware to use this event loop
        for middleware in redis_broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        ev_loop.run_until_complete(self.init_worker())

        # Start a Dramatiq worker in a separate thread
        worker = Worker(redis_broker, queues=[CACHER_QUEUE_NAME])
        worker_thread = threading.Thread(target=worker.start, daemon=True)
        self._worker_thread = worker_thread  # Store the thread object
        worker_thread.start()

        health_reporter_task = ev_loop.create_task(self._periodic_health_reporter())

        try:
            ev_loop.run_forever()
        finally:
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                ev_loop.run_until_complete(asyncio.sleep(2))
            ev_loop.close()


if __name__ == '__main__':
    cacher = Cacher('Cacher')
    cacher.run()
