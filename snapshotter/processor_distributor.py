import asyncio
import importlib
import json
import multiprocessing
import queue
import resource
import sys
import time
import traceback
from collections import defaultdict
from rpc_helper.rpc import RpcHelper
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from socket import gethostname
from typing import Awaitable, Tuple
from typing import Dict
from typing import List
from typing import Set
from uuid import uuid4

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker
from eth_utils.address import to_checksum_address
from eth_utils.crypto import keccak
from httpx import Client as SyncClient
from httpx import HTTPTransport
from httpx import Limits
from httpx import Timeout
from redis import asyncio as aioredis
from web3 import Web3

from snapshotter.health_ping import create_health_ping_actor
from snapshotter.health_ping import run_periodic_broker_health_check
from snapshotter.settings.config import aggregator_config
from snapshotter.settings.config import preloaders
from snapshotter.settings.config import projects_config
from snapshotter.settings.config import aggregator_types
from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import send_telegram_notification_sync
from snapshotter.utils.data_utils import get_source_chain_epoch_size
from snapshotter.utils.data_utils import get_source_chain_id
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import SnapshotterReportState
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.data_models import TelegramEpochProcessingReportMessage
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.models.message_models import ProcessingCompleteMessage
from snapshotter.utils.models.message_models import CalculateAggregateMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping, event_detector_last_processed_block
from snapshotter.trade_volume_worker import trade_volume_aggregator
from snapshotter.metadata_worker import metadata_fetcher
from snapshotter.timeseries_worker import timeseries_aggregator
from snapshotter.cross_project_worker import cross_project_aggregator
from snapshotter.utils.dramatiq_queues import (
    EVENT_DETECTOR_QUEUE_NAME,
    DISTRIBUTOR_HEALTH_QUEUE_NAME,
    SNAPSHOT_QUEUE_NAME,
    AGGREGATION_QUEUE_NAME,
    CACHER_QUEUE_NAME,
    TRADE_VOLUME_WORKER_QUEUE_NAME,
    METADATA_WORKER_QUEUE_NAME,
    TIMESERIES_WORKER_QUEUE_NAME,
    CROSS_PROJECT_WORKER_QUEUE_NAME,
)

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


class ProcessorDistributor(multiprocessing.Process):
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
        super(ProcessorDistributor, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._logger = default_logger.bind(
            module=f'ProcessDistributor:{settings.namespace}-{settings.instance_id}',
        )
        self._q = queue.Queue()
        self._shutdown_initiated = False

        self._initialized = False

        self._upcoming_project_changes = defaultdict(list)
        self._preload_completion_conditions: Dict[int, Dict] = defaultdict(
            dict,
        )  # epoch ID to preloading complete event

        self._shutdown_initiated = False
        self._all_preload_tasks = set()
        self._project_type_config_mapping = dict()
        for project_config in projects_config:
            self._project_type_config_mapping[project_config.project_name] = project_config
            for preload_task in project_config.preload_tasks:
                self._all_preload_tasks.add(preload_task)

        self._aggregator_config_mapping = dict()
        for agg_config in aggregator_config:
            self._aggregator_config_mapping[agg_config.project_name] = agg_config

        self._logger.debug('All preload tasks by string ID during init: {}', self._all_preload_tasks)
        self._last_epoch_processing_health_check = 0
        self._preloader_compute_mapping = dict()
        self._snapshot_build_awaited_project_ids = dict()
        # Task tracking
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval

        self._handle_event_actor = dramatiq.actor(
            queue_name=EVENT_DETECTOR_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

        # Initialize reporting and notification related attributes
        self._telegram_httpx_client = None
        self.notification_cooldown = settings.reporting.min_reporting_interval
        self.last_notification_time = 0

        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._health_ping_actor = create_health_ping_actor(
            broker=redis_broker,
            queue_name=DISTRIBUTOR_HEALTH_QUEUE_NAME,
            actor_name='healthPingDist',
            logger=self._logger
        )

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

    async def _init_preloader_compute_mapping(self):
        """
        Initializes the preloader compute mapping by importing the preloader module and class and
        adding it to the mapping dictionary.
        """
        if self._preloader_compute_mapping:
            return

        for preloader in preloaders:
            if preloader.task_type in self._all_preload_tasks:
                preloader_module = importlib.import_module(preloader.module)
                self._logger.debug('Imported preloader module: {}', preloader_module)
                preloader_class = getattr(preloader_module, preloader.class_name)
                self._preloader_compute_mapping[preloader.task_type] = preloader_class
                self._logger.debug(
                    'Imported preloader class {} against preloader module {} for task type {}',
                    preloader_class,
                    preloader_module,
                    preloader.task_type,
                )

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
        try:
            source_block_time = await self._protocol_state_contract.functions.SOURCE_CHAIN_BLOCK_TIME(
                Web3.to_checksum_address(settings.data_market),
            ).call()
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for source chain block time: {}',
                e,
            )
            sys.exit(1)
        else:
            self._source_chain_block_time = source_block_time / 10 ** 4
            self._logger.debug('Set source chain block time to {}', self._source_chain_block_time)

        try:
            epoch_size = await self._protocol_state_contract.functions.EPOCH_SIZE(
                Web3.to_checksum_address(settings.data_market),
            ).call()
        except Exception as e:
            self._logger.exception(
                'Exception in querying protocol state for epoch size: {}',
                e,
            )
            sys.exit(1)
        else:
            self._epoch_size = epoch_size
            self._logger.debug('Set epoch size to {}', self._epoch_size)
        self._epochs_in_a_day = 86400 // (self._epoch_size * self._source_chain_block_time)
        self._logger.debug('Set epochs in a day to {}', self._epochs_in_a_day)
        try:
            self._source_chain_epoch_size = await get_source_chain_epoch_size(
            redis_conn=self._redis_conn,
                state_contract_obj=self._protocol_state_contract,
                rpc_helper=self._anchor_rpc_helper,
            )
        except Exception as e:
            self._logger.error(f'Error fetching source chain epoch size in processor distributor _init_protocol_meta: {e}')
            sys.exit(1)

        try:
            self._source_chain_id = await get_source_chain_id(
                redis_conn=self._redis_conn,
                rpc_helper=self._anchor_rpc_helper,
                state_contract_obj=self._protocol_state_contract,
            )
        except Exception as e:
            self._logger.error(f'Error fetching source chain id in processor distributor _init_protocol_meta: {e}')
            sys.exit(1)

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
            await self._init_preloader_compute_mapping()
            self._logger.debug('Initialized preloader compute mapping in Processor Distributor init_worker')
            await self._init_protocol_meta()
            asyncio.create_task(self._cleanup_tasks())
            self._init_httpx_client()
            self._logger.debug('Initialized httpx client in Processor Distributor init_worker')

        self._initialized = True

    async def _preloader_waiter(
        self,
        epoch: EpochBase,
    ):
        """
        Wait for all preloading tasks to complete for the given epoch, and distribute snapshot build tasks if all preloading
        dependencies are satisfied.

        Args:
            epoch: The epoch for which to wait for preloading tasks to complete.

        Returns:
            None
        """
        preloader_types_l = list(self._preload_completion_conditions[epoch.epochId].keys())
        conditions: List[Awaitable] = [
            self._preload_completion_conditions[epoch.epochId][k]
            for k in preloader_types_l
        ]
        self._logger.info(
            'Waiting for preload conditions against epoch {}: {}',
            epoch.epochId,
            conditions,
        )
        preload_results = await asyncio.gather(
            *conditions,
            return_exceptions=True,
        )
        succesful_preloads = list()
        failed_preloads = list()
        self._logger.debug(
            'Preloading asyncio gather returned with results {} for epoch {}',
            preload_results,
            epoch.epochId,
        )
        for i, preload_result in enumerate(preload_results):
            if isinstance(preload_result, Exception):
                self._logger.error(
                    f'Preloading failed for epoch {epoch.epochId} project type {preloader_types_l[i]}',
                )
                failed_preloads.append(preloader_types_l[i])
            else:
                succesful_preloads.append(preloader_types_l[i])
                self._logger.debug(
                    'Preloading successful for preloader {} for epoch {}',
                    preloader_types_l[i],
                    epoch.epochId,
                )

        self._logger.debug('Final list of successful preloads: {} for epoch {}', succesful_preloads, epoch.epochId)
        for project_name in self._project_type_config_mapping:
            project_config = self._project_type_config_mapping[project_name]
            if not project_config.preload_tasks:
                continue
            self._logger.debug(
                'Expected list of successful preloading for project name {} epoch {}: {}',
                project_name,
                epoch.epochId,
                project_config.preload_tasks,
            )
            if all([t in succesful_preloads for t in project_config.preload_tasks]):
                self._logger.info(
                    'Preloading dependency satisfied for project name {} epoch {}. Distributing snapshot build tasks...',
                    project_name, epoch.epochId,
                )
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(epoch.epochId, SnapshotterStates.PRELOAD.value),
                    mapping={
                        project_name: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).model_dump_json(),
                    },
                )
                await self._distribute_callbacks_snapshotting(project_name, epoch)
            else:
                self._logger.error(
                    'Preloading dependency not satisfied for project name {} epoch {}. Not distributing snapshot build tasks...',
                    project_name, epoch.epochId,
                )
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(epoch.epochId, SnapshotterStates.PRELOAD.value),
                    mapping={
                        project_name: SnapshotterStateUpdate(
                            status='failed', timestamp=int(time.time()),
                        ).model_dump_json(),
                    },
                )
        # TODO: set separate overall status for failed and successful preloads
        if epoch.epochId in self._preload_completion_conditions:
            del self._preload_completion_conditions[epoch.epochId]

    async def _exec_preloaders(
        self, msg_obj: EpochBase,
    ):
        """
        Executes preloading tasks for the given epoch object.

        Args:
            msg_obj (EpochBase): The epoch object for which preloading tasks need to be executed.

        Returns:
            None
        """
        # Cleanup previous preloading complete tasks and events
        # Start all preload tasks
        self._logger.debug('Starting all preload tasks for epoch {}: {}', msg_obj.epochId, self._all_preload_tasks)
        for preloader in preloaders:
            if preloader.task_type in self._all_preload_tasks:
                preloader_class = self._preloader_compute_mapping[preloader.task_type]
                preloader_obj = preloader_class()
                preloader_compute_kwargs = dict(
                    epoch=msg_obj,
                    redis_conn=self._redis_conn,
                    rpc_helper=self._rpc_helper,
                )
                self._logger.debug(
                    'Starting preloader obj {} for epoch {}',
                    preloader.task_type,
                    msg_obj.epochId,
                )
                f = preloader_obj.compute(**preloader_compute_kwargs)
                self._preload_completion_conditions[msg_obj.epochId][preloader.task_type] = f
                self._logger.debug(
                    'Preloader future {} against task type {} for epoch {} started',
                    f,
                    preloader.task_type,
                    msg_obj.epochId,
                )

        current_time = time.time()
        preloader_task = asyncio.create_task(
            self._preloader_waiter(
                epoch=msg_obj,
            ),
            name=f'preloader_waiter_epoch_{msg_obj.epochId}',
        )
        preloader_task_tuple = (current_time, preloader_task)
        self._active_tasks.add(preloader_task_tuple)
        preloader_task.add_done_callback(lambda t: self._handle_task_result(t, preloader_task_tuple))

    async def _epoch_release_processor(self, event_data):
        """
        This method is called when an epoch is released. It enables pending projects for the epoch and executes preloaders.

        Args:
            message (IncomingMessage): The message containing the epoch information.
        """
        msg_obj: EpochBase = (
            EpochBase.model_validate_json(event_data)
        )

        self._logger.debug('Pushing epoch release to preloader coroutine: {}', msg_obj)
        current_time = time.time()
        task = asyncio.create_task(
            self._exec_preloaders(msg_obj=msg_obj),
            name=f'exec_preloaders_epoch_{msg_obj.epochId}',
        )

        task_tuple = (current_time, task)
        self._active_tasks.add(task_tuple)
        task.add_done_callback(lambda t: self._handle_task_result(t, task_tuple))

        # Handle all projects without preload tasks
        for project_name, project_config in self._project_type_config_mapping.items():
            if not project_config.preload_tasks:
                # Release for snapshotting
                current_time = time.time()
                task = asyncio.create_task(
                    self._distribute_callbacks_snapshotting(
                        project_name, msg_obj,
                    ),
                    name=f'distribute_snapshotting_{project_name}_epoch_{msg_obj.epochId}',
                )
                task_tuple = (current_time, task)
                self._active_tasks.add(task_tuple)
                task.add_done_callback(lambda t: self._handle_task_result(t, task_tuple))

    async def _distribute_callbacks_snapshotting(self, project_name: str, epoch: EpochBase):
        """
        Distributes callbacks for snapshotting to the appropriate snapshotters based on the project name and epoch.

        Args:
            project_type (str): The type of project.
            epoch (EpochBase): The epoch to snapshot.

        Returns:
            None
        """
        process_unit = SnapshotProcessMessage(
            begin=epoch.begin,
            end=epoch.end,
            epochId=epoch.epochId,
        )

        project_type = project_name.split(':')[0]
        dramatiq.broker.get_broker().enqueue(
            dramatiq.Message(
                queue_name=f'{SNAPSHOT_QUEUE_NAME}-{project_type}',
                actor_name='handleEvent',  # Match actor name with event_receiver.py
                args=(project_name, process_unit.model_dump_json()),
                kwargs={},
                options={},
            ),
        )
        self._logger.info(
            'Sent out message to be processed by worker'
            f' {project_name} : {process_unit}',
        )

    async def _distribute_downstream_tasks(self, event_data):
        """
        Distributes downstream tasks for aggregation and other post-processing.
        """
        self._logger.debug('Distributing downstream tasks for: {}', event_data)
        process_unit: ProcessingCompleteMessage = (
            ProcessingCompleteMessage.model_validate_json(event_data)
        )

        # Existing aggregation logic
        if len(aggregator_types) > 0:
            for config in aggregator_config:
                task_type = config.project_name
                if config.depends_on in process_unit.task_type:
                    calculate_aggregate_message = CalculateAggregateMessage(
                        epochId=process_unit.epochId,
                        begin=process_unit.begin,
                        end=process_unit.end,
                        task_type=task_type,
                        processed_message=process_unit,
                    )
                    dramatiq.broker.get_broker().enqueue(
                        dramatiq.Message(
                            queue_name=AGGREGATION_QUEUE_NAME,
                            actor_name='handleEvent',
                            args=(task_type, calculate_aggregate_message.model_dump_json()),
                            kwargs={},
                            options={},
                        ),
                    )
        else:
            self._logger.debug('No aggregator types found, skipping aggregation distribution')

        # New logic for trade volume aggregation
        if 'baseSnapshot:uniswap-v3:trades' in process_unit.task_type:
            self._logger.info(f"Enqueuing task for TradeVolumeWorker for {process_unit.task_type}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=TRADE_VOLUME_WORKER_QUEUE_NAME,
                    actor_name='process_volume_aggregation_actor',
                    args=(process_unit.model_dump(),
                          self._rpc_helper.model_dump(),
                          self._anchor_rpc_helper.model_dump(),
                          self._ipfs_reader_client.model_dump(),
                          self._protocol_state_contract.model_dump()),
                    kwargs={},
                    options={},
                ),
            )

        # New logic for metadata fetching
        if 'activePools' in process_unit.task_type:
            self._logger.info(f"Enqueuing task for MetadataWorker for {process_unit.task_type}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=METADATA_WORKER_QUEUE_NAME,
                    actor_name='process_metadata_fetching_actor',
                    args=({'task_type': 'activePools', 'epochId': process_unit.epochId},
                          self._rpc_helper.model_dump(),
                          self._anchor_rpc_helper.model_dump(),
                          self._ipfs_reader_client.model_dump(),
                          self._protocol_state_contract.model_dump()),
                    kwargs={},
                    options={},
                ),
            )
        
        if 'activeTokens' in process_unit.task_type:
            self._logger.info(f"Enqueuing task for MetadataWorker for {process_unit.task_type}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=METADATA_WORKER_QUEUE_NAME,
                    actor_name='process_metadata_fetching_actor',
                    args=({'task_type': 'activeTokens', 'epochId': process_unit.epochId},
                          self._rpc_helper.model_dump(),
                          self._anchor_rpc_helper.model_dump(),
                          self._ipfs_reader_client.model_dump(),
                          self._protocol_state_contract.model_dump()),
                    kwargs={},
                    options={},
                ),
            )

        # New logic for time series aggregation
        if process_unit.task_type.startswith('baseSnapshot:') or process_unit.task_type.startswith('tradesSnapshot:'):
            self._logger.info(f"Enqueuing task for TimeSeriesWorker for {process_unit.task_type}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=TIMESERIES_WORKER_QUEUE_NAME,
                    actor_name='process_timeseries_aggregation_actor',
                    args=({'task_type': process_unit.task_type, 'epochId': process_unit.epochId, 'projectId': process_unit.projectId},
                          self._rpc_helper.model_dump(),
                          self._anchor_rpc_helper.model_dump(),
                          self._ipfs_reader_client.model_dump(),
                          self._protocol_state_contract.model_dump()),
                    kwargs={},
                    options={},
                ),
            )

        # New logic for cross-project aggregation
        if process_unit.task_type.startswith('baseSnapshot:') or \
           process_unit.task_type.startswith('tradesSnapshot:') or \
           process_unit.task_type.startswith('tokenPools:'):
            self._logger.info(f"Enqueuing task for CrossProjectWorker for {process_unit.task_type}")
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=CROSS_PROJECT_WORKER_QUEUE_NAME,
                    actor_name='process_cross_project_aggregation_actor',
                    args=({'task_type': process_unit.task_type, 'epochId': process_unit.epochId, 'projectId': process_unit.projectId},
                          self._rpc_helper.model_dump(),
                          self._anchor_rpc_helper.model_dump(),
                          self._ipfs_reader_client.model_dump(),
                          self._protocol_state_contract.model_dump()),
                    kwargs={},
                    options={},
                ),
            )


    async def _cleanup_older_epoch_status(self, epoch_id: int):
        """
        Deletes the epoch status keys for the epoch that is 30 epochs older than the given epoch_id.
        """
        tasks = []
        delete_keys = list()
        for state in SnapshotterStates:
            k = epoch_id_project_to_state_mapping(epoch_id - 30, state.value)
            delete_keys.append(k)
        if delete_keys:
            tasks.append(self._redis_conn.delete(*delete_keys))
        await asyncio.gather(*tasks, return_exceptions=True)

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

        if event_type == 'EpochReleased':
            epoch_msg: EpochBase = EpochBase.model_validate_json(event_data)
            current_time = time.time()
            task = asyncio.create_task(
                self._cleanup_older_epoch_status(epoch_msg.epochId),
                name=f'cleanup_epoch_{epoch_msg.epochId - 30}',
            )
            task_tuple = (current_time, task)
            self._active_tasks.add(task_tuple)
            task.add_done_callback(lambda t: self._handle_task_result(t, task_tuple))

            await self._epoch_release_processor(event_data)

        elif event_type == 'SnapshotSubmitted':
            # enqueue to cacher
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=CACHER_QUEUE_NAME,
                    actor_name='handleEvent',
                    args=(event_type, event_data),
                    kwargs={},
                    options={},
                ),
            )
        elif event_type == 'ProcessingComplete':
            await self._distribute_downstream_tasks(
                event_data,
            )

        elif event_type == 'SnapshotFinalized':
            # enqueue to cacher
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=CACHER_QUEUE_NAME,
                    actor_name='handleEvent',
                    args=(event_type, event_data),
                    kwargs={},
                    options={},
                ),
            )

        elif event_type == 'SnapshotBatchSubmitted':
            # enqueue to cacher
            dramatiq.broker.get_broker().enqueue(
                dramatiq.Message(
                    queue_name=CACHER_QUEUE_NAME,
                    actor_name='handleEvent',
                    args=(event_type, event_data),
                    kwargs={},
                    options={},
                ),
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
            self._send_telegram_epoch_processing_notification(e)

    def _handle_task_result(self, task: asyncio.Task, task_tuple: tuple):
        """Callback function to handle task completion, log exceptions, and clean up."""
        try:
            exception = task.exception()
            if exception:
                task_name = task.get_name() if hasattr(task, 'get_name') else 'Unnamed task'
                error_traceback = ''.join(
                    traceback.format_exception(type(exception), exception, exception.__traceback__),
                )
                self._logger.error(f"Task '{task_name}' failed: {exception}", exc_info=exception)
                self._logger.error(f"Detailed traceback for task '{task_name}':\n{error_traceback}")
                self._send_telegram_epoch_processing_notification(exception)
        except asyncio.CancelledError:
            task_name = task.get_name() if hasattr(task, 'get_name') else 'Unnamed task'
            self._logger.warning(f"Task '{task_name}' was cancelled.")
        except Exception as e:
            # Catch potential errors within the callback itself
            error_traceback = ''.join(
                traceback.format_exception(type(e), e, e.__traceback__),
            )
            self._logger.error(f'Error in task result handler: {e}', exc_info=e)
            self._logger.error(f'Detailed traceback for handler error:\n{error_traceback}')
        finally:
            # Ensure cleanup happens even if the callback logic has an error
            self._active_tasks.discard(task_tuple)

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
                        f'Task {task} timed out. Cancelling..., current_time: {current_time}, start_time: {task_start_time}',
                    )
                    task.cancel()
                    self._active_tasks.discard((task_start_time, task))

    def _init_httpx_client(self):
        """
        Initializes the httpx client for sending Telegram notifications.
        """
        # Initialize HTTP client for Telegram notifications
        self._telegram_httpx_client = SyncClient(
            base_url=settings.reporting.telegram_url,
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=HTTPTransport(
                limits=Limits(
                    max_connections=100,
                    max_keepalive_connections=50,
                    keepalive_expiry=None,
                ),
            ),
        )

    def _send_telegram_epoch_processing_notification(
        self,
        error: Exception,
    ):
        """
        Send a Telegram notification about epoch processing errors.

        This method constructs and sends a detailed error notification via Telegram
        when epoch processing encounters issues. The notification includes instance
        details, error information, and current status.

        Args:
            error (Exception): The error that occurred during processing

        Raises:
            Various exceptions possible during HTTP requests
        """

        if (int(time.time()) - self.last_notification_time) >= self.notification_cooldown and \
                (settings.reporting.telegram_url and settings.reporting.telegram_chat_id):

            if not self._telegram_httpx_client:
                self._logger.error('Telegram client not initialized')
                return

            try:
                # Format the error with detailed traceback information
                error_traceback = ''.join(
                    traceback.format_exception(type(error), error, error.__traceback__),
                )

                telegram_message = TelegramEpochProcessingReportMessage(
                    chatId=settings.reporting.telegram_chat_id,
                    slotId=settings.slot_id,
                    issue=SnapshotterIssue(
                        instanceID=settings.instance_id,
                        issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                        projectID='',
                        epochId='',
                        timeOfReporting=str(time.time()),
                        extra=json.dumps({'issueDetails': f'Error: {error}\n\nTraceback:\n{error_traceback}'}),
                    ),
                )

                send_telegram_notification_sync(
                    client=self._telegram_httpx_client,
                    message=telegram_message,
                )

                self.last_notification_time = int(time.time())
            except Exception as e:
                self._logger.error('Error sending Telegram notification: {}', e)

    def run(self) -> None:
        """
        Runs the ProcessorDistributor by setting resource limits, registering signal handlers,
        initializing the worker, starting the Dramatiq worker's internal threads, and running the event loop.
        """
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ev_loop)

        ProcessorDistributor._event_loop = ev_loop  # Store the event loop
        
        # Update the middleware to use this event loop
        for middleware in redis_broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        # Initialize worker components
        ev_loop.run_until_complete(self.init_worker())
        worker = Worker(redis_broker, queues=[EVENT_DETECTOR_QUEUE_NAME, DISTRIBUTOR_HEALTH_QUEUE_NAME])
        
        self._logger.info("Starting Distributor Dramatiq worker internal threads...")
        worker.start()

        health_reporter_task = ev_loop.create_task(
            run_periodic_broker_health_check(
                logger=self._logger,
                redis_conn=self._redis_conn,
                hostname=self._hostname,
                health_report_interval=self._health_report_interval,
                health_actor_send=self._health_ping_actor.send,
                worker_type="ProcessorDistributor",
                health_queue_name=DISTRIBUTOR_HEALTH_QUEUE_NAME
            )
        )

        try:
            self._logger.info("Running Distributor main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("Distributor main event loop stopped. Shutting down...")
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                try:
                    ev_loop.run_until_complete(asyncio.sleep(1))
                except RuntimeError as e:
                     self._logger.warning(f"Could not fully await health reporter cancellation on loop close: {e}") # Corrected logger usage

            try:
                self._logger.info("Stopping Distributor Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("Distributor Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping Distributor Dramatiq worker: {e}")
            
            self._logger.info("Closing Distributor event loop...")
            ev_loop.close()
            self._logger.info("Distributor Event loop closed.")


if __name__ == '__main__':
    processor_distributor = ProcessorDistributor('ProcessorDistributor')
    processor_distributor.run()
