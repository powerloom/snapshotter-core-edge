import asyncio
import importlib
import resource
import time
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from socket import gethostname

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware import AsyncIO
from dramatiq.worker import Worker
from pydantic import ValidationError

from snapshotter.health_ping import create_health_ping_actor
from snapshotter.health_ping import run_periodic_broker_health_check
from snapshotter.settings.config import aggregator_config
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.generic_worker import GenericAsyncWorker
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import CalculateAggregateMessage
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.dramatiq_queues import AGGREGATION_QUEUE_NAME, AGGREGATION_HEALTH_QUEUE_NAME


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

        self._logger = default_logger.bind(module='AggregationWorker')

        self._project_calculation_mapping = None
        self._task_types = set()

        # Categorize project types based on aggregation configuration
        for config in aggregator_config:
            self._task_types.add(config.project_name)

        self._handle_event_actor = dramatiq.actor(
            queue_name=AGGREGATION_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        # Bind logger once for the instance
        self._health_ping_actor = create_health_ping_actor(
            broker=redis_broker,
            queue_name=AGGREGATION_HEALTH_QUEUE_NAME,
            actor_name='healthPingAgg',
            logger=self._logger # Pass the instance logger
        )

    async def _process_task(
        self,
        msg_obj: CalculateAggregateMessage,
        task_type: str,
    ):
        """
        Process the given message object and task type.

        This method handles the core logic of processing a task, including
        error handling, state updates, and snapshot creation.

        Args:
            msg_obj (Union[SnapshotSubmittedMessage, CalculateAggregateMessage]):
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

        try:
            self._logger.info(
                'Got epoch to process for {}: {}',
                task_type, msg_obj,
            )

            task_processor = self._project_calculation_mapping[task_type]

            # Compute the snapshot
            snapshots = await task_processor.compute(
                msg_obj=msg_obj,
                redis_conn=self._redis_conn,
                rpc_helper=self._rpc_helper,
                anchor_rpc_helper=self._anchor_rpc_helper,
                ipfs_reader=self._ipfs_reader_client,
                protocol_state_contract=self._protocol_state_contract,
                task_type=task_type,
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
                    f"{task_type}:{settings.namespace}": SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).model_dump_json(),
                },
            )
        else:
            if not snapshots:
                # Handle empty snapshot case
                await self._redis_conn.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                    ),
                    mapping={
                        f"{task_type}:{settings.namespace}": SnapshotterStateUpdate(
                            status='failed', timestamp=int(time.time()), error='Empty snapshot',
                        ).model_dump_json(),
                    },
                )
            else:
                for project_id, snapshot in snapshots:
                    p = self._redis_conn.pipeline()
                    p.hset(
                        name=epoch_id_project_to_state_mapping(
                            epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                        ),
                        mapping={
                            project_id: SnapshotterStateUpdate(
                                status='success', timestamp=int(time.time()),
                            ).model_dump_json(),
                        },
                    )
                    await p.execute()
                    await self._commit_payload(
                        task_type=task_type,
                        project_id=project_id,
                        epoch=msg_obj,
                        snapshot=snapshot,
                        _ipfs_writer_client=self._ipfs_writer_client,
                    )

    def handle_event(self, *args):
        """
        Handle an event.
        """
        self._logger.debug('Handling event: {}', args)
        try:
            event_type = args[0]
            event_data = args[1]

            msg_obj: CalculateAggregateMessage = (
                CalculateAggregateMessage.model_validate_json(event_data)
            )
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
            key = project_config.project_name
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
        Runs the worker by setting resource limits, registering signal handlers, starting the Dramatiq worker's
        internal threads, and running the main event loop until it is stopped.
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
        self._event_loop = ev_loop

        # Update the middleware to use this event loop
        for middleware in redis_broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        self._logger.debug(
            f'Starting Aggregation worker {self._unique_id}...',
        )

        self._event_loop.run_until_complete(self.init_worker())
        worker = Worker(redis_broker, queues=[AGGREGATION_QUEUE_NAME, AGGREGATION_HEALTH_QUEUE_NAME])

        self._logger.info("Starting Aggregator Dramatiq worker internal threads...")
        worker.start()

        # Start the centralized health reporter task
        health_reporter_task = self._event_loop.create_task(
            run_periodic_broker_health_check(
                logger=self._logger,
                redis_conn=self._redis_conn,
                hostname=self._hostname,
                health_report_interval=self._health_report_interval,
                health_actor_send=self._health_ping_actor.send,
                worker_type="AggregatorWorker",
                health_queue_name=AGGREGATION_HEALTH_QUEUE_NAME
            )
        )

        try:
            self._logger.info("Running Aggregator main event loop...")
            self._event_loop.run_forever()
        finally:
            self._logger.info("Aggregator main event loop stopped. Shutting down...")
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                try:
                    self._event_loop.run_until_complete(asyncio.sleep(1))
                except RuntimeError as e:
                    self._logger.warning(f"Could not fully await health reporter cancellation on loop close: {e}")

            try:
                self._logger.info("Stopping Aggregator Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("Aggregator Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping Aggregator Dramatiq worker: {e}")

            self._logger.info("Closing Aggregator event loop...")
            self._event_loop.close()
            self._logger.info("Aggregator Event loop closed.")


if __name__ == '__main__':
    aggregation_worker = AggregationAsyncWorker('AggregationAsyncWorker')
    aggregation_worker.run()
