import asyncio
import multiprocessing
import resource
import time
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Set, Optional
from uuid import uuid4
from socket import gethostname

import tenacity
import uvloop
from eth_utils.crypto import keccak
from ipfs_client.dag import IPFSAsyncClientError
from ipfs_client.main import AsyncIPFSClient
from ipfs_client.main import AsyncIPFSClientSingleton
from tenacity import retry
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import snapshots_to_unpin_zset_name
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key

logger = default_logger.bind(module='IPFSUnpinningWorker')


class IPFSUnpinningWorker(multiprocessing.Process):
    """
    A worker class for unpinning snapshots from IPFS.

    This worker periodically checks for snapshots that are ready to be unpinned
    from IPFS based on a configured time delay. It manages the lifecycle of IPFS
    content by removing pins after they've been stored for a sufficient time.
    """
    _active_tasks: Set[asyncio.Task]
    _ipfs_singleton: AsyncIPFSClientSingleton
    _ipfs_writer_client: AsyncIPFSClient
    _ipfs_reader_client: AsyncIPFSClient
    _unpin_snapshots_task: Optional[asyncio.Task]

    def __init__(self, name, **kwargs):
        """
        Initializes an IPFSUnpinningWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to pass to the superclass constructor.
        """
        # Generate a unique ID for this worker instance
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]

        super(IPFSUnpinningWorker, self).__init__(name=name, **kwargs)

        # Initialization state tracking
        self._initialized = False

        # Task tracking collection and configuration
        self._active_tasks: Set[asyncio.Task] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval
        self._event_loop = None
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._unpin_snapshots_task = None

    def _signal_handler(self, signum, frame):
        """
        Signal handler function that handles shutdown when a SIGINT, SIGTERM or SIGQUIT signal is received.

        This method sets a flag to initiate a graceful shutdown when the process receives
        termination signals.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info('Shutdown initiated')

    @retry(
        wait=wait_random_exponential(multiplier=1, max=10),
        stop=stop_after_attempt(5),
        retry=tenacity.retry_if_not_exception_type(IPFSAsyncClientError),
        reraise=True,
    )
    async def _upload_to_ipfs(self, snapshot: bytes, _ipfs_writer_client: AsyncIPFSClient):
        """
        Uploads a snapshot to IPFS using the provided AsyncIPFSClient.

        This method adds the snapshot to IPFS and, if unpinning is enabled, schedules it
        for future unpinning by adding it to a Redis sorted set with a score representing
        the time when it should be unpinned.

        Args:
            snapshot (bytes): The snapshot data to upload.
            _ipfs_writer_client (AsyncIPFSClient): The IPFS client to use for uploading.

        Returns:
            str: The CID (Content Identifier) of the uploaded snapshot.
        """
        # Upload the snapshot to IPFS
        snapshot_cid = await _ipfs_writer_client.add_bytes(snapshot)

        # If unpinning is enabled, schedule this snapshot for future unpinning
        if settings.ipfs_unpinning.enabled:
            # Add to redis zset of unpinned snapshots with a score of current time + unpin delay
            await self._redis_conn.zadd(
                name=snapshots_to_unpin_zset_name(),
                mapping={snapshot_cid: time.time() + settings.ipfs_unpinning.unpin_after},
            )
        return snapshot_cid

    async def _unpin_snapshot(self, snapshot_cid: str):
        """
        Unpins a snapshot from IPFS.

        This method removes the pin for a specific snapshot CID from IPFS and
        removes the CID from the Redis sorted set tracking unpinned snapshots.

        Args:
            snapshot_cid (str): The CID of the snapshot to unpin.
        """
        # Remove the pin from IPFS but skip S3 removal
        try:
            await self._ipfs_writer_client.remove_bytes(snapshot_cid, skip_s3_removal=False)
        except Exception as e:
            self._logger.error(f'Error unpinning snapshot {snapshot_cid}: {e}, file may not exist in IPFS')

        # Remove the CID from the Redis sorted set tracking unpinned snapshots
        await self._redis_conn.zrem(snapshots_to_unpin_zset_name(), snapshot_cid)
        self._logger.info(f'Unpinned snapshot {snapshot_cid}')

    async def _unpin_snapshots(self):
        """
        Unpins all snapshots from IPFS that are ready to be unpinned.

        This method retrieves all snapshot CIDs from the Redis sorted set that have scores
        (timestamps) less than or equal to the current time, indicating they are ready
        to be unpinned. For each CID, it creates a tracked task to unpin the snapshot.

        The method runs in an infinite loop, checking for snapshots to unpin every 10 minutes.

        Returns:
            None

        Note:
            This method uses asyncio.run_coroutine_threadsafe to ensure that the tracked tasks
            are properly created and managed within the event loop, even if this method is
            called from a different thread.
        """
        while True:
            # Get all snapshot CIDs from the Redis sorted set with scores (timestamps)
            # less than or equal to the current time, meaning they're ready to be unpinned
            current_time = int(time.time())
            snapshot_cids = await self._redis_conn.zrange(
                name=snapshots_to_unpin_zset_name(),
                start=0,  # Start from the lowest score
                end=current_time,  # Up to the current time
            )
            self._logger.info(f'Found {len(snapshot_cids)} snapshots to unpin')

            # Process each snapshot that needs to be unpinned
            for snapshot_cid in snapshot_cids:
                # Create a tracked task for unpinning and ensure it's properly scheduled
                # in the event loop using run_coroutine_threadsafe
                # future = asyncio.run_coroutine_threadsafe(
                #     self._create_tracked_task(self._unpin_snapshot(snapshot_cid)),
                #     self._event_loop,
                # )
                # # Wait for the task creation to complete
                # future.result()

                # NOTE: Using basic await call for now, will switch to something fancy if needed later.
                await self._unpin_snapshot(snapshot_cid.decode('utf-8'))

            # Wait for 10 minutes (600 seconds) before checking again for snapshots to unpin
            # This reduces load on Redis and IPFS while still ensuring timely unpinning
            self._logger.info('Waiting for 10 minutes before checking again for snapshots to unpin')
            await asyncio.sleep(600)

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool.

        This method creates a Redis connection pool and assigns it to the instance
        for use in other methods.
        """
        # Create and populate the Redis pool
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()

        # Store the connection for use in other methods
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _create_tracked_task(self, task):
        """
        Creates and tracks an asynchronous task.

        This method creates a new task from the given coroutine, adds it to the set of active tasks,
        and sets up a callback to remove the task from the set when it's completed.

        Args:
            task (Coroutine): The coroutine to be executed as a task.

        Returns:
            asyncio.Task: The created task.

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

        return new_task

    async def _init_ipfs_client(self):
        """
        Initialize the IPFS client.

        This method creates a singleton instance of AsyncIPFSClientSingleton,
        initializes its sessions, and assigns the write and read clients to instance variables.
        """
        # Create the IPFS client singleton
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)

        # Initialize the IPFS client sessions
        await self._ipfs_singleton.init_sessions()

        # Store references to the write and read clients
        self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def init(self):
        """
        Initializes the worker by setting up required components.

        This method initializes the IPFS client, Redis pool, and starts the task cleanup process.
        """
        if not self._initialized:
            # Initialize components
            await self._init_ipfs_client()
            await self._init_redis_pool()

            # Start the task cleanup process
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True

    async def _cleanup_tasks(self):
        """
        Periodically clean up completed or timed-out tasks.

        This method runs in the background and periodically checks for tasks that have
        completed or timed out, removing them from the active tasks set.
        """
        while True:
            # Wait for the configured cleanup interval
            await asyncio.sleep(self._task_cleanup_interval)

            # Check each active task
            for task_start_time, task in list(self._active_tasks):
                current_time = time.time()

                # Remove completed tasks
                if task.done():
                    self._active_tasks.discard((task_start_time, task))
                # Cancel and remove timed-out tasks
                elif current_time - task_start_time > self._task_timeout:
                    self._logger.warning(
                        f'Task {task} timed out. Cancelling..., current_time: {current_time}, '
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
            if not self._unpin_snapshots_task or self._unpin_snapshots_task.done():
                should_report = False
                if self._unpin_snapshots_task:
                    # Task is done, check for exception
                    exc = self._unpin_snapshots_task.exception()
                    if exc:
                        self._logger.error(
                            f'Main unpin task failed with exception: {exc}. Halting health reports.'
                        )
                    else:
                        self._logger.warning(
                            'Main unpin task finished unexpectedly. Halting health reports.'
                        )
                    # Halt the health reporter if the main task is done (failed or finished)
                    break
                else:
                    # Task hasn't started yet or was never assigned
                    self._logger.warning('Main unpin task not found. Skipping health report for now.')
                    # Don't break here, the task might start later

            try:
                if should_report:
                    await self.report_health_status()
                # else: Task is not running or not found yet, skip reporting

                await asyncio.sleep(self._health_report_interval)
            except asyncio.CancelledError:
                self._logger.info(f'Periodic health reporter task for {self._hostname} cancelled.')
                break
            except Exception as e:
                self._logger.error(f'Error in periodic health reporter loop: {e}')
                # Optionally add a small delay before retrying after an error
                await asyncio.sleep(self._health_report_interval) # Keep the interval consistent even after error

    def run(self) -> None:
        """
        Runs the worker process.

        This method sets up resource limits, registers signal handlers, initializes
        the event loop, and starts the worker's main functionality.
        """
        # Set up logging
        self._logger = logger

        # Set resource limits for file descriptors
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Register signal handlers for graceful shutdown
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)

        # Set up the event loop with uvloop for better performance
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        # Create a new event loop and set it as the current one
        ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ev_loop)
        self._event_loop = ev_loop

        self._logger.debug(
            f'Starting IPFS unpinning worker {self._unique_id}...',
        )

        # Initialize the worker
        self._event_loop.run_until_complete(self.init())

        # Start the event detection loop
        # self._event_loop.run_until_complete(self._unpin_snapshots())
        health_reporter_task = self._event_loop.create_task(self._periodic_health_reporter())
        self._unpin_snapshots_task = self._event_loop.create_task(self._unpin_snapshots())

        try:
            # Wait for the unpinning task to complete (it runs indefinitely)
            self._event_loop.run_until_complete(self._unpin_snapshots_task)
        finally:
            # Gracefully handle shutdown
            shutdown_tasks = []
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                shutdown_tasks.append(health_reporter_task)
            if self._unpin_snapshots_task and not self._unpin_snapshots_task.done():
                self._unpin_snapshots_task.cancel()
                shutdown_tasks.append(self._unpin_snapshots_task)

            # Allow some time for tasks to clean up
            if shutdown_tasks:
                try:
                    # Gather cancelled tasks to ensure they complete cancellation
                    self._event_loop.run_until_complete(asyncio.gather(*shutdown_tasks, return_exceptions=True))
                except RuntimeError as e:
                    self._logger.warning(f"Could not fully await task cancellations on loop close: {e}")

            self._event_loop.close()


if __name__ == '__main__':
    ipfs_unpinning_worker = IPFSUnpinningWorker('IPFSUnpinningWorker')
    ipfs_unpinning_worker.run()
