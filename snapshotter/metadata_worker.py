import asyncio
import multiprocessing
import time
import traceback
from typing import Dict, Set, Tuple, Optional
import threading
import resource
from redis import asyncio as aioredis
import threading

import dramatiq
import uvloop
from dramatiq.brokers.redis import RedisBroker
from dramatiq.middleware.asyncio import AsyncIO
from dramatiq.worker import Worker
from signal import SIGINT, SIGTERM, SIGQUIT, signal

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from computes.api.utils.data_utils import get_uniswap_v3_pool_metadata, get_token_metadata
from snapshotter.utils.data_utils import get_project_latest_snapshot
from snapshotter.utils.redis.redis_keys import metadata_pool_key, metadata_token_key
from snapshotter.utils.dramatiq_queues import METADATA_WORKER_QUEUE_NAME


class MetadataWorker(multiprocessing.Process):
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _event_loop = None
    _active_tasks: Set[Tuple[float, asyncio.Task]]
    _worker_thread: Optional[threading.Thread]

    def __init__(self, name, **kwargs):
        super(MetadataWorker, self).__init__(name=name, **kwargs)
        self._logger = default_logger.bind(module="MetadataWorker")
        self._shutdown_initiated = False
        self._initialized = False
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval
        self._worker_thread: Optional[threading.Thread] = None

        # Register the handle_event method as a Dramatiq actor
        self._handle_event_actor = dramatiq.actor(
            queue_name=METADATA_WORKER_QUEUE_NAME,
            actor_name='handleEvent',
        )(self.handle_event)

        self.broker = RedisBroker(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
        )
        self.broker.add_middleware(AsyncIO())
        dramatiq.set_broker(self.broker)

    async def _init_redis_pool(self):
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def init_worker(self):
        if not self._initialized:
            await self._init_redis_pool()
            self._logger.debug('Initialized Redis pool in MetadataWorker init_worker')
            asyncio.create_task(self._cleanup_tasks())
        self._initialized = True

    def _signal_handler(self, signum, frame):
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info('Shutdown initiated')

    async def _create_tracked_task(self, task):
        current_time = time.time()
        new_task = asyncio.create_task(task)
        self._active_tasks.add((current_time, new_task))
        new_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, new_task)))

    async def _cleanup_tasks(self):
        while True:
            try:
                await asyncio.sleep(self._task_cleanup_interval)
                current_time = time.time()
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
                        self._active_tasks.discard((task_start_time, task))
            except asyncio.CancelledError:
                self._logger.info("Task cleanup loop cancelled")
                break
            except Exception as e:
                self._logger.error(f"Error in task cleanup loop: {e}")
                await asyncio.sleep(self._task_cleanup_interval)

    @dramatiq.actor(queue_name=METADATA_WORKER_QUEUE_NAME)
    def handle_event(self, *args):
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
        self._logger.info(
            (
                'Got message to process and distribute: {}'
            ),
            event_data,
        )

        if event_type == 'MetadataFetch':
            self._logger.info(f'MetadataFetch event caught with message {event_data}')
            await self._create_tracked_task(self._process_metadata_fetching_async(event_data))
        else:
            self._logger.error(
                (
                    'Unknown message type: {}'
                ),
                event_type,
            )

        if self._redis_conn:
            await self._redis_conn.close()

    async def _process_metadata_fetching_async(self, payload: Dict):
        redis_conn = self._redis_conn
        task_type = payload.get('task_type').split(':')[0]
        

        snapshot = await get_project_latest_snapshot(
            redis_conn,
            project_id=project_id
        )

        if not snapshot:
            self._logger.warning(f"Could not find snapshot for {project_id}")
            return

        if task_type == 'activePools':
            addresses = snapshot.get('pools', {}).keys()
            asset_type = 'pool'
            fetch_func = get_uniswap_v3_pool_metadata
        elif task_type == 'activeTokens':
            addresses = snapshot.get('tokens', {}).keys()
            asset_type = 'token'
            fetch_func = get_token_metadata
        else:
            self._logger.warning(f"Unknown task type for metadata worker: {task_type}")
            return

        for address in addresses:
            cache_key = metadata_pool_key(address) if asset_type == 'pool' else metadata_token_key(address)
            cached_data = await redis_conn.get(cache_key)
            if cached_data:
                continue

            self._logger.info(f"Fetching metadata for {asset_type} {address}")
            try:
                metadata = await fetch_func(
                    redis_conn=redis_conn,
                    pool_address=address if asset_type == 'pool' else None,
                    token_address=address if asset_type == 'token' else None,
                )
                if metadata:
                    await redis_conn.set(cache_key, metadata.model_dump_json(), ex=86400) # Cache for 24 hours
            except Exception as e:
                self._logger.error(f"Error fetching metadata for {asset_type} {address}: {e}", exc_info=True)

    def run(self) -> None:
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
            MetadataWorker._event_loop = ev_loop  # Store the event loop
            # Update the middleware to use this event loop
            for middleware in self.broker.middleware:
                if isinstance(middleware, dramatiq.middleware.AsyncIO):
                    middleware.event_loop = ev_loop

            # Initialize the worker
            ev_loop.run_until_complete(self.init_worker())

            # Start a Dramatiq worker in a separate thread
            worker = Worker(self.broker, queues=[METADATA_WORKER_QUEUE_NAME])
            worker_thread = threading.Thread(target=worker.start, daemon=True)
            self._worker_thread = worker_thread  # Store the thread object
            worker_thread.start()

            try:
                # Run the event loop until shutdown is requested
                ev_loop.run_forever()
            finally:
                # Close Redis connection
                if hasattr(self, '_redis_conn') and self._redis_conn:
                    ev_loop.run_until_complete(self._redis_conn.close())
                
                ev_loop.close()
        except Exception as e:
            self._logger.error(f"Fatal error in MetadataWorker process: {e}")
            self._logger.error(traceback.format_exc())
            raise


if __name__ == '__main__':
    worker = MetadataWorker('MetadataWorker')
    worker.run()
