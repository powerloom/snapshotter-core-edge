import asyncio
import multiprocessing
import resource
import time
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Set, Optional, List, Any
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

from snapshotter.health_ping import run_periodic_task_health_check
from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import snapshots_to_unpin_zset_name
from redis import asyncio as aioredis
from typing import Coroutine
from asyncio import AbstractEventLoop

# Initialize module-level logger
logger: Any = default_logger.bind(module='IPFSUnpinningWorker')


class IPFSUnpinningWorker(multiprocessing.Process):
    """
    A worker class for unpinning snapshots from IPFS.

    This worker periodically checks for snapshots that are ready to be unpinned
    from IPFS based on a configured time delay. It manages the lifecycle of IPFS
    content by removing pins after they've been stored for a sufficient time.

    Attributes:
        _active_tasks (Set[tuple[float, asyncio.Task]]): Set of currently running tasks with their start times
        _ipfs_singleton (AsyncIPFSClientSingleton): Singleton instance of IPFS client
        _ipfs_writer_client (AsyncIPFSClient): Client for writing to IPFS
        _ipfs_reader_client (AsyncIPFSClient): Client for reading from IPFS
        _unpin_snapshots_task (Optional[asyncio.Task]): Main task for unpinning snapshots
        _redis_conn (aioredis.Redis): Redis connection instance
        _aioredis_pool (RedisPoolCache): Redis connection pool
        _logger (Logger): Logger instance for this worker
        _event_loop (AbstractEventLoop): Event loop for async operations
    """
    _active_tasks: Set[tuple[float, asyncio.Task]]
    _ipfs_singleton: AsyncIPFSClientSingleton
    _ipfs_writer_client: AsyncIPFSClient
    _ipfs_reader_client: AsyncIPFSClient
    _unpin_snapshots_task: Optional[asyncio.Task]
    _redis_conn: aioredis.Redis
    _aioredis_pool: RedisPoolCache
    _logger: Any
    _event_loop: AbstractEventLoop

    def __init__(self, name: str, **kwargs) -> None:
        """
        Initializes an IPFSUnpinningWorker instance.

        Args:
            name (str): The name of the worker.
            **kwargs: Additional keyword arguments to pass to the superclass constructor.
        """
        # Generate a unique ID for this worker instance
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]

        super(IPFSUnpinningWorker, self).__init__(args=(), kwargs={'name': name, **kwargs})

        # Initialization state tracking
        self._initialized = False

        # Task tracking collection and configuration
        self._active_tasks = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._unpin_snapshots_task = None
        self._shutdown_initiated = False

    def _signal_handler(self, signum: int, frame: Any) -> None:
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
    async def _upload_to_ipfs(self, snapshot: bytes, _ipfs_writer_client: AsyncIPFSClient) -> str:
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
        snapshot_cid: str = await _ipfs_writer_client.add_bytes(snapshot)

        # If unpinning is enabled, schedule this snapshot for future unpinning
        if settings.ipfs_unpinning.enabled:
            # Add to redis zset of unpinned snapshots with a score of current time + unpin delay
            await self._redis_conn.zadd(
                name=snapshots_to_unpin_zset_name(),
                mapping={snapshot_cid: time.time() + settings.ipfs_unpinning.unpin_after},
            )
        return snapshot_cid

    async def _unpin_snapshot(self, snapshot_cid: str) -> None:
        """
        Unpins a snapshot from IPFS.

        This method removes the pin for a specific snapshot CID from IPFS and
        removes the CID from the Redis sorted set tracking unpinned snapshots.

        Args:
            snapshot_cid (str): The CID of the snapshot to unpin.
        """
        # Remove the pin from IPFS but skip S3 removal
        try:
            await self._ipfs_writer_client.remove_bytes(snapshot_cid, skip_s3_removal=True)
        except Exception as e:
            self._logger.error(f'Error unpinning snapshot {snapshot_cid}: {e}, file may not exist in IPFS')

        # Remove the CID from the Redis sorted set tracking unpinned snapshots
        await self._redis_conn.zrem(snapshots_to_unpin_zset_name(), snapshot_cid)
        self._logger.info(f'Unpinned snapshot {snapshot_cid}')

    async def _unpin_snapshots(self) -> None:
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
            current_time: int = int(time.time())
            snapshot_cids: List[bytes] = await self._redis_conn.zrange(
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

    async def _init_redis_pool(self) -> None:
        """
        Initializes the Redis connection pool for the worker.

        This method creates and populates a Redis connection pool using RedisPoolCache.
        The pool is then stored in the instance for use across other methods.

        Returns:
            None

        Note:
            This method should be called during worker initialization to ensure
            Redis connectivity is established before any operations requiring Redis.
        """
        # Create and populate the Redis pool with connection settings
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()

        # Store the active Redis connection for use in other methods
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def _create_tracked_task(self, task: Coroutine) -> asyncio.Task:
        """
        Creates and tracks an asynchronous task with lifecycle management.

        This method creates a new task from the given coroutine, adds it to the set of active tasks
        with its creation timestamp, and sets up automatic cleanup when the task completes.

        Args:
            task (Coroutine): The coroutine to be executed as a task.

        Returns:
            asyncio.Task: The created and tracked task.

        Note:
            Tasks created through this method are automatically tracked for:
            - Monitoring active tasks
            - Cleanup of completed tasks
            - Timeout detection and cancellation
        """
        # Record the task creation timestamp for timeout tracking
        current_time: float = time.time()

        # Create a new task from the provided coroutine
        new_task: asyncio.Task = asyncio.create_task(task)

        # Add the task to the tracking set with its creation time
        self._active_tasks.add((current_time, new_task))

        # Configure automatic cleanup when the task completes
        new_task.add_done_callback(lambda _: self._active_tasks.discard((current_time, new_task)))

        return new_task

    async def _init_ipfs_client(self) -> None:
        """
        Initializes the IPFS client with read and write capabilities.

        This method creates a singleton instance of AsyncIPFSClientSingleton,
        initializes its sessions, and sets up separate clients for read and write operations.

        Returns:
            None

        Note:
            The IPFS client is initialized as a singleton to ensure consistent
            connection management across the application.
        """
        # Initialize the IPFS client singleton with configuration
        self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)

        # Set up the IPFS client sessions
        await self._ipfs_singleton.init_sessions()

        # Configure separate clients for read and write operations
        self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
        self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client

    async def init(self) -> None:
        """
        Initializes the worker's core components and services.

        This method performs one-time initialization of required components:
        - IPFS client for data operations
        - Redis connection pool for state management
        - Task cleanup service for resource management

        Returns:
            None

        Note:
            This method is idempotent - subsequent calls will not reinitialize
            components if they are already initialized.
        """
        if not self._initialized:
            # Initialize core components
            await self._init_ipfs_client()
            await self._init_redis_pool()

            # Launch the task cleanup service
            asyncio.create_task(self._cleanup_tasks())

        self._initialized = True

    async def _cleanup_tasks(self) -> None:
        """
        Manages the lifecycle of asynchronous tasks.

        This method runs continuously in the background to:
        - Remove completed tasks from tracking
        - Cancel and clean up timed-out tasks
        - Maintain system resource efficiency

        Returns:
            None

        Note:
            The cleanup process runs at intervals defined by _task_cleanup_interval
            to balance system load and responsiveness.
        """
        while True:
            # Wait for the next cleanup cycle
            await asyncio.sleep(self._task_cleanup_interval)

            # Process all tracked tasks
            for task_start_time, task in list(self._active_tasks):
                current_time: float = time.time()

                # Handle completed tasks
                if task.done():
                    self._active_tasks.discard((task_start_time, task))
                # Handle timed-out tasks
                elif current_time - task_start_time > self._task_timeout:
                    self._logger.warning(
                        f'Task {task} timed out. Cancelling..., current_time: {current_time}, '
                        f'start_time: {task_start_time}',
                    )
                    task.cancel()
                    self._active_tasks.discard((task_start_time, task))

    def run(self) -> None:
        """
        Executes the main worker process with proper resource management.

        This method orchestrates the worker's lifecycle:
        1. Sets up logging and resource limits
        2. Configures signal handling for graceful shutdown
        3. Initializes the event loop with performance optimizations
        4. Starts core services (unpinning and health monitoring)
        5. Manages graceful shutdown of all components

        Returns:
            None

        Note:
            The method implements proper resource cleanup and graceful shutdown
            handling to ensure system stability.
        """
        # Configure logging for the worker
        self._logger = logger

        # Set system resource limits for file descriptors
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Set up signal handlers for graceful shutdown
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)

        # Configure high-performance event loop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        # Initialize and set the event loop
        ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ev_loop)
        self._event_loop = ev_loop

        self._logger.debug(
            f'Starting IPFS unpinning worker {self._unique_id}...',
        )

        # Initialize worker components
        self._event_loop.run_until_complete(self.init())

        # Launch the main unpinning service
        self._unpin_snapshots_task = self._event_loop.create_task(self._unpin_snapshots())

        # Start the health monitoring service
        health_reporter_task = self._event_loop.create_task(
            run_periodic_task_health_check(
                logger=self._logger,
                redis_conn=self._redis_conn,
                hostname=self._hostname,
                health_report_interval=self._health_report_interval,
                main_task=self._unpin_snapshots_task
            )
        )

        try:
            # Run the main service loop
            self._event_loop.run_until_complete(self._unpin_snapshots_task)
        finally:
            # Prepare for graceful shutdown
            shutdown_tasks: List[asyncio.Task] = []

            # Cancel and track health monitoring task
            if health_reporter_task and not health_reporter_task.done():
                health_reporter_task.cancel()
                shutdown_tasks.append(health_reporter_task)

            # Cancel and track main service task
            if self._unpin_snapshots_task and not self._unpin_snapshots_task.done():
                self._unpin_snapshots_task.cancel()
                shutdown_tasks.append(self._unpin_snapshots_task)

            # Execute graceful shutdown of all tasks
            if shutdown_tasks:
                try:
                    self._event_loop.run_until_complete(asyncio.gather(*shutdown_tasks, return_exceptions=True))
                except RuntimeError as e:
                    self._logger.warning(f"Could not fully await task cancellations on loop close: {e}")

            # Clean up the event loop
            self._event_loop.close()


if __name__ == '__main__':
    # Initialize and run the IPFS unpinning worker
    ipfs_unpinning_worker: IPFSUnpinningWorker = IPFSUnpinningWorker('IPFSUnpinningWorker')
    ipfs_unpinning_worker.run()
