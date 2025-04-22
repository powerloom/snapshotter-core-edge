import asyncio
import hashlib
import importlib
import resource
import threading
import time
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Union

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker
from pydantic import ValidationError

from snapshotter.settings.config import aggregator_config
from snapshotter.settings.config import projects_config
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.generic_worker import GenericAsyncWorker
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.models.settings_model import AggregateOn
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping

AGGREGATION_QUEUE_NAME = f'powerloom-aggregator_{settings.namespace}_{settings.instance_id}'
logger = default_logger.bind(module='AggregationWorker')

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


class AggregationAsyncWorker(GenericAsyncWorker):
    """
    A worker class for asynchronous aggregation tasks.

    This class extends GenericAsyncWorker and provides functionality for
    processing aggregation tasks, managing IPFS clients, and handling
    project-specific calculations.
    """

    def __init__(self, name, **kwargs):
        """
        Initialize an instance of AggregationAsyncWorker.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the parent class constructor.
        """
        super(AggregationAsyncWorker, self).__init__(name=name, **kwargs)

        self._project_calculation_mapping = None
        self._single_project_types = set()
        self._multi_project_types = set()
        self._task_types = set()

        # Categorize project types based on aggregation configuration
        for config in aggregator_config:
            if config.aggregate_on == AggregateOn.single_project:
                self._single_project_types.add(config.project_type)
            elif config.aggregate_on == AggregateOn.multi_project:
                self._multi_project_types.add(config.project_type)
            self._task_types.add(config.project_type)

        self._handle_event_actor = dramatiq.actor(
            queue_name=AGGREGATION_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

    def _gen_single_type_project_id(self, task_type, epoch):
        """
        Generate a project ID for a single task type and epoch.

        Args:
            task_type (str): The task type.
            epoch (Epoch): The epoch object.

        Returns:
            str: The generated project ID.
        """
        data_source = epoch.projectId.split(':')[-2]
        project_id = f'{task_type}:{data_source}:{settings.namespace}'
        return project_id

    def _gen_multiple_type_project_id(self, task_type, epoch):
        """
        Generate a unique project ID based on the task type and epoch messages.

        Args:
            task_type (str): The type of task.
            epoch (Epoch): The epoch object containing messages.

        Returns:
            str: The generated project ID.
        """
        underlying_project_ids = [project.projectId for project in epoch.messages]
        unique_project_id = ''.join(sorted(underlying_project_ids))

        project_hash = hashlib.sha3_256(unique_project_id.encode()).hexdigest()

        project_id = f'{task_type}:{project_hash}:{settings.namespace}'
        return project_id

    def _gen_project_id(self, task_type, epoch):
        """
        Generate a project ID based on the given task type and epoch.

        Args:
            task_type (str): The type of task.
            epoch (int): The epoch number.

        Returns:
            str: The generated project ID.

        Raises:
            ValueError: If the task type is unknown.
        """
        if task_type in self._single_project_types:
            return self._gen_single_type_project_id(task_type, epoch)
        elif task_type in self._multi_project_types:
            return self._gen_multiple_type_project_id(task_type, epoch)
        else:
            raise ValueError(f'Unknown project type {task_type}')

    async def _process_task(
        self,
        msg_obj: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        task_type: str,
    ):
        """
        Process the given message object and task type.

        This method handles the core logic of processing a task, including
        error handling, state updates, and snapshot creation.

        Args:
            msg_obj (Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]):
                The message object to be processed.
            task_type (str): The type of task to be performed.

        Returns:
            None
        """
        self._logger.debug(
            'Processing callback: {}', msg_obj,
        )

        if task_type not in self._project_calculation_mapping:
            self._logger.error(
                (
                    'No project calculation mapping found for task type'
                    f' {task_type}. Skipping...'
                ),
            )
            return

        project_id = self._gen_project_id(task_type, msg_obj)

        try:
            self._logger.info(
                'Got epoch to process for {}: {}',
                task_type, msg_obj,
            )

            task_processor = self._project_calculation_mapping[task_type]

            # Compute the snapshot
            snapshot = await task_processor.compute(
                msg_obj=msg_obj,
                redis=self._redis_conn,
                rpc_helper=self._rpc_helper,
                anchor_rpc_helper=self._anchor_rpc_helper,
                ipfs_reader=self._ipfs_reader_client,
                protocol_state_contract=self._protocol_state_contract,
                project_id=project_id,
            )

        except Exception as e:
            # Handle exceptions during processing
            self._logger.opt(exception=settings.logs.debug_mode).error(
                'Exception processing callback for epoch: {}, Error: {},'
                'sending failure notifications', msg_obj, e,
            )

            # Update Redis with failure state
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                ),
                mapping={
                    project_id: SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).json(),
                },
            )
        else:
            if not snapshot:
                # Handle empty snapshot case
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='failed', timestamp=int(time.time()), error='Empty snapshot',
                        ).json(),
                    },
                )
            else:
                # Handle successful snapshot case
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).json(),
                    },
                )
                await self._commit_payload(
                    task_type=task_type,
                    project_id=project_id,
                    epoch=msg_obj,
                    snapshot=snapshot,
                    _ipfs_writer_client=self._ipfs_writer_client,
                )
            self._logger.debug(
                'Updated epoch processing status in aggregation worker for project {} for transition {}',
                project_id, SnapshotterStates.SNAPSHOT_BUILD.value,
            )
        await self._redis_conn.close()

    def handle_event(self, *args):
        """
        Handle an event.
        """
        self._logger.debug('Handling event: {}', args)
        event_type = args[0]
        event_data = args[1]
        try:
            if event_type in self._single_project_types:
                msg_obj: PowerloomSnapshotSubmittedMessage = PowerloomSnapshotSubmittedMessage.parse_raw(event_data)
            elif event_type in self._multi_project_types:
                msg_obj: PowerloomCalculateAggregateMessage = PowerloomCalculateAggregateMessage.parse_raw(event_data)
            else:
                self._logger.error('Unknown event type: {}', event_type)
                return
        except ValidationError as e:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                (
                    'Bad message structure of callback processor. Error: {}, {}'
                ),
                e, event_data,
            )
            return
        except Exception as e:
            self._logger.opt(exception=settings.logs.debug_mode).error(
                (
                    'Unexpected message structure of callback in processor. Error: {}'
                ),
                e,
            )
            return
        else:
            if msg_obj.epochId == 0:
                self._logger.debug('Skipping aggregation snapshot for epoch 0. Incoming msg: {}', msg_obj)
                return

        future = asyncio.run_coroutine_threadsafe(
            self._create_tracked_task(self._process_task(msg_obj=msg_obj, task_type=event_type)),
            self._event_loop,
        )
        future.result()

    async def _init_project_calculation_mapping(self):
        """
        Initialize the project calculation mapping.

        This method imports the processor module and class for each project type
        specified in the aggregator and projects configuration. It raises an
        exception if a duplicate project type is found.
        """
        if self._project_calculation_mapping is not None:
            return

        self._project_calculation_mapping = dict()
        for project_config in aggregator_config:
            key = project_config.project_type
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()
        for project_config in projects_config:
            key = project_config.project_type
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()

    async def init_worker(self):
        """
        Initialize the worker.

        This method sets up the project calculation mapping, IPFS client,
        and other necessary components for the worker to function.
        """
        if not self._initialized:
            await self._init_project_calculation_mapping()
            await self.init()

    def run(self) -> None:
        """
        Runs the worker by setting resource limits, registering signal handlers, starting the Dramatiq worker, and
        running the event loop until it is stopped.
        """
        self._logger = logger
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.get_event_loop()
        self._event_loop = ev_loop

        # Update the middleware to use this event loop
        for middleware in redis_broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        self._logger.debug(
            f'Starting asynchronous callback worker {self._unique_id}...',
        )

        self._event_loop.run_until_complete(self.init_worker())

        # Start a Dramatiq worker in a separate thread
        worker = Worker(redis_broker, queues=[AGGREGATION_QUEUE_NAME])
        worker_thread = threading.Thread(target=worker.start, daemon=True)
        worker_thread.start()
        try:
            ev_loop.run_forever()
        finally:
            ev_loop.close()


if __name__ == '__main__':
    aggregation_worker = AggregationAsyncWorker('AggregationAsyncWorker')
    aggregation_worker.run()
