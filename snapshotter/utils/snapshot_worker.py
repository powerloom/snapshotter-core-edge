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

from snapshotter.settings.config import projects_config
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.generic_worker import GenericAsyncWorker
from snapshotter.utils.models.data_models import SnapshotterStates
from snapshotter.utils.models.data_models import SnapshotterStateUpdate
from snapshotter.utils.models.message_models import SnapshotProcessMessage
from snapshotter.utils.redis.redis_keys import epoch_id_project_to_state_mapping
from snapshotter.utils.redis.redis_keys import last_snapshot_processing_complete_timestamp_key
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key
from snapshotter.utils.redis.redis_keys import submitted_base_snapshots_key

SNAPSHOT_QUEUE_NAME = f'powerloom-snapshotter_{settings.namespace}_{settings.instance_id}'
logger = default_logger.bind(module='SnapshotWorker')

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

HEALTH_QUEUE_NAME = f'powerloom-snapshotter-health_{settings.namespace}_{settings.instance_id}'

@dramatiq.actor(broker=redis_broker, queue_name=HEALTH_QUEUE_NAME, actor_name='healthPingSnapshot')
def health_ping(hostname: str):
    """Simple actor that updates the main service health timestamp key for a given hostname."""
    if not hostname:
        logger.error("Received snapshot health ping request without a hostname.")
        return
    try:
        # Use the sync redis client available within dramatiq actors
        redis_conn = redis_broker.client
        key = service_health_timestamps_key()
        current_timestamp = int(time.time())
        redis_conn.hset(key, hostname, current_timestamp)
        logger.debug(f'Snapshot health ping processed for hostname: {hostname} at {current_timestamp}')
    except Exception as e:
        logger.error(f"Error in snapshot health_ping actor for hostname {hostname}: {e}")

class SnapshotAsyncWorker(GenericAsyncWorker):
    """
    A worker class for asynchronous snapshot processing.

    This class extends GenericAsyncWorker and provides functionality for processing
    snapshot tasks asynchronously, including IPFS operations and project-specific calculations.
    """

    def __init__(self, name, **kwargs):
        """
        Initialize a SnapshotAsyncWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to be passed to the GenericAsyncWorker constructor.
        """
        super(SnapshotAsyncWorker, self).__init__(name=name, **kwargs)
        self._project_calculation_mapping = None
        self._task_types = []
        for project_config in projects_config:
            task_type = project_config.project_name
            self._task_types.append(task_type)
        self._handle_event_actor = dramatiq.actor(
            queue_name=SNAPSHOT_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval

    async def _process(self, msg_obj: SnapshotProcessMessage, task_type: str):
        """
        Process snapshots in bulk mode.

        This method handles the computation and storage of multiple snapshots at once.

        Args:
            msg_obj (SnapshotProcessMessage): The message object containing snapshot task details.
            task_type (str): The type of task to be performed.

        Raises:
            Exception: If an error occurs while processing the snapshots.
        """
        try:
            # Get the task processor for the given task type
            task_processor = self._project_calculation_mapping[task_type]
            
            # Compute snapshots in bulk
            snapshots = await task_processor.compute(
                epoch=msg_obj,
                redis_conn=self._redis_conn,
                rpc_helper=self._rpc_helper,
                anchor_rpc_helper=self._anchor_rpc_helper,
                ipfs_reader=self._ipfs_reader_client,
                protocol_state_contract=self._protocol_state_contract,
                task_type=task_type,
            )

            if not snapshots:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )

        except Exception as e:
            # Handle exceptions during bulk snapshot processing
            self._logger.opt(exception=True).error(
                'Exception processing callback for epoch: {}, task_type: {}, Error: {},'
                'sending failure notifications', msg_obj, task_type, e,
            )

            # Update Redis with failure state
            await self._redis_conn.hset(
                name=epoch_id_project_to_state_mapping(
                    epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                ),
                mapping={
                    f'{task_type}:{settings.namespace}': SnapshotterStateUpdate(
                        status='failed', error=str(e), timestamp=int(time.time()),
                    ).json(),
                },
            )
            await self._send_failure_notifications(error=e, epoch_id=msg_obj.epochId, project_id='bulk_mode')
        else:
            # Handle successful bulk snapshot processing
            await self._redis_conn.set(
                name=last_snapshot_processing_complete_timestamp_key(),
                value=int(time.time()),
            )

            if not snapshots:
                self._logger.debug(
                    'No snapshot data for: {}, skipping...', msg_obj,
                )
                return

            self._logger.info('Sending snapshots to commit service: {}', snapshots)

            # Process each snapshot in the bulk result
            for project_id, snapshot in snapshots:
                # Store snapshot in Redis
                await self._redis_conn.set(
                    name=submitted_base_snapshots_key(
                        epoch_id=msg_obj.epochId, project_id=project_id,
                    ),
                    value=snapshot.json(),
                    # Store snapshot for 10 mins
                    ex=600,
                )

                # Update Redis with success state
                p = self._redis_conn.pipeline()
                p.hset(
                    name=epoch_id_project_to_state_mapping(
                        epoch_id=msg_obj.epochId, state_id=SnapshotterStates.SNAPSHOT_BUILD.value,
                    ),
                    mapping={
                        project_id: SnapshotterStateUpdate(
                            status='success', timestamp=int(time.time()),
                        ).json(),
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

    async def _process_task(self, msg_obj: SnapshotProcessMessage, task_type: str):
        """
        Process a SnapshotProcessMessage object for a given task type.

        This method initializes necessary components and delegates the processing
        to either single mode or bulk mode based on the message object.

        Args:
            msg_obj (SnapshotProcessMessage): The message object to process.
            task_type (str): The type of task to perform.
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

        self._logger.info(
            'Got epoch to process for {}: {}',
            task_type, msg_obj,
        )

        await self._process(msg_obj=msg_obj, task_type=task_type)

        await self._redis_conn.close()

    def handle_event(self, *args):
        """
        Handle an event.
        """
        self._logger.debug('Handling event: {}', args)
        try:
            event_type = args[0]
            event_data = args[1]

            msg_obj: SnapshotProcessMessage = (
                SnapshotProcessMessage.parse_raw(event_data)
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
        future = asyncio.run_coroutine_threadsafe(
            self._create_tracked_task(self._process_task(msg_obj=msg_obj, task_type=event_type)),
            self._event_loop,
        )
        future.result()

    async def _init_project_calculation_mapping(self):
        """
        Initialize the project calculation mapping.

        This method creates a dictionary that maps project types to their corresponding
        calculation classes based on the projects configuration.

        Raises:
            Exception: If a duplicate project type is found in the projects configuration.
        """
        if self._project_calculation_mapping is not None:
            return
        # Generate project function mapping
        self._project_calculation_mapping = dict()
        for project_config in projects_config:
            key = project_config.project_name
            if key in self._project_calculation_mapping:
                raise Exception('Duplicate project type found')
            module = importlib.import_module(project_config.processor.module)
            class_ = getattr(module, project_config.processor.class_name)
            self._project_calculation_mapping[key] = class_()

    async def init_worker(self):
        """
        Initialize the worker.

        This method initializes the project calculation mapping, IPFS client,
        and other necessary components if they haven't been initialized yet.
        """
        if not self._initialized:
            await self._init_project_calculation_mapping()
            await self.init()

    async def _periodic_health_reporter(self):
        """Periodically triggers and checks Dramatiq worker liveness via health ping."""
        self._logger.info(
            f'Starting periodic Dramatiq health check task for {self._hostname} (Interval: {self._health_report_interval}s)',
        )
        dramatiq_liveness_threshold = self._health_report_interval + 30
        health_key = service_health_timestamps_key()

        # Allow a grace period on startup before reporting critical errors
        startup_grace_period_end = time.time() + dramatiq_liveness_threshold + 10

        while True:
            try:
                health_ping.send(self._hostname)
                self._logger.debug(f"Sent snapshot health ping for {self._hostname} to {HEALTH_QUEUE_NAME}")

                # Check the timestamp set by the health_ping actor in Redis for our hostname
                last_ping_time_bytes = await self._redis_conn.hget(health_key, self._hostname)
                current_time = int(time.time())

                if last_ping_time_bytes:
                    last_ping_time = int(last_ping_time_bytes.decode())
                    time_since_last_ping = current_time - last_ping_time
                    if time_since_last_ping > dramatiq_liveness_threshold:
                        # Only log critical after grace period
                        if current_time > startup_grace_period_end:
                            self._logger.critical(
                                f"Dramatiq workers seem unresponsive for {self._hostname}. Last health ping acknowledged {time_since_last_ping}s ago"
                                f" (Threshold: {dramatiq_liveness_threshold}s). Key: {health_key}, Field: {self._hostname}"
                            )
                        else:
                             self._logger.debug(
                                f"Dramatiq workers potentially unresponsive for {self._hostname} (in startup grace period). Last health ping acknowledged {time_since_last_ping}s ago."
                             )
                    else:
                        self._logger.debug(
                            f"Dramatiq workers appear responsive for {self._hostname}. Last health ping acknowledged {time_since_last_ping}s ago."
                        )
                else:
                    # If the key/field doesn't exist yet, maybe the first ping hasn't been processed.
                     if current_time > startup_grace_period_end:
                        self._logger.debug(
                            f"Dramatiq worker health timestamp for {self._hostname} not found. Workers might be starting up or unresponsive. Key: {health_key}"
                        )
                     else:
                        self._logger.debug(
                             f"Dramatiq worker health timestamp for {self._hostname} not yet found (in startup grace period). Key: {health_key}"
                         )

                await asyncio.sleep(self._health_report_interval)
            except asyncio.CancelledError:
                self._logger.info(f'Periodic health reporter task for {self._hostname} cancelled.')
                break
            except Exception as e:
                self._logger.error(f'Error in periodic health reporter loop: {e}')
                await asyncio.sleep(self._health_report_interval)

    def run(self) -> None:
        """
        Runs the worker by setting resource limits, registering signal handlers, starting the Dramatiq worker's
        internal threads, and running the main event loop until it is stopped.
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

        # Create a new event loop and set it as the current one
        ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ev_loop)
        self._event_loop = ev_loop

        # Update the middleware to use this event loop
        for middleware in redis_broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        self._logger.debug(
            f'Starting asynchronous callback worker {self._unique_id}...',
        )

        # init worker components
        self._event_loop.run_until_complete(self.init_worker())
        worker = Worker(redis_broker, queues=[SNAPSHOT_QUEUE_NAME, HEALTH_QUEUE_NAME])

        # Start the worker's internal threads
        self._logger.info("Starting Dramatiq worker internal threads...")
        worker.start()

        health_reporter_task = self._event_loop.create_task(self._periodic_health_reporter())

        try:
            self._logger.info("Running main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("Main event loop stopped. Shutting down...")
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                try:
                    ev_loop.run_until_complete(asyncio.sleep(1))
                except RuntimeError as e:
                    self._logger.warning(f"Could not fully await health reporter cancellation on loop close: {e}")

            # Stop the Dramatiq worker's internal threads
            try:
                self._logger.info("Stopping Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping Dramatiq worker: {e}")

            # Close the event loop
            self._logger.info("Closing event loop...")
            ev_loop.close()
            self._logger.info("Event loop closed.")


if __name__ == '__main__':
    snapshot_worker = SnapshotAsyncWorker('SnapshotAsyncWorker')
    snapshot_worker.run()
