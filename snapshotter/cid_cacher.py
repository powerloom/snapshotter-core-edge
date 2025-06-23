import json
import asyncio
import multiprocessing
import resource
import traceback
import os
from rpc_helper.rpc import RpcHelper
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from typing import Dict
from typing import List
from typing import Set
from typing import Tuple
from typing import Optional
from uuid import uuid4
from ipfs_client.main import AsyncIPFSClientSingleton

import uvloop
from eth_utils.crypto import keccak
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import cids_to_cache_set
from snapshotter.utils.data_utils import PROJECT_DATA_ENTRY_EXPIRY
from snapshotter.utils.data_utils import get_submission_data
from snapshotter.utils.redis.redis_keys import cid_cache


class CidCacher(multiprocessing.Process):
    
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
        """
        super(CidCacher, self).__init__(name=name, **kwargs)
        self._unique_id = f'{name}-' + keccak(text=str(uuid4())).hex()[:8]
        self._logger = default_logger.bind(
            module=f'Cacher:{settings.namespace}-{settings.instance_id}',
        )
        self._shutdown_initiated = False
        self._initialized = False
        self._hostname = f"{os.getpid()}-{settings.instance_id}"

        # Task tracking
        self._active_tasks: Set[Tuple[float, asyncio.Task]] = set()
        self._task_timeout = settings.async_task_config.task_timeout
        self._task_cleanup_interval = settings.async_task_config.task_cleanup_interval
        self._caching_interval = 60
        self._cid_cache_expiry = PROJECT_DATA_ENTRY_EXPIRY

        # IPFS client attributes
        self._ipfs_singleton: Optional[AsyncIPFSClientSingleton] = None
        self._ipfs_writer_client = None
        self._ipfs_reader_client = None

    def _signal_handler(self, signum, frame):
        """
        Signal handler method that handles shutdown when a SIGINT, SIGTERM, or SIGQUIT signal is received.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame at the time the signal was received.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info(f'Shutdown initiated by signal {signum}')
            
            # Cancel all active tasks gracefully
            if hasattr(self, '_event_loop') and self._event_loop:
                self._event_loop.call_soon_threadsafe(self._event_loop.stop)

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and populates it with connections.
        
        This method creates a new RedisPoolCache instance and establishes connections
        to the Redis server based on the configuration in settings.
        """
        try:
            self._aioredis_pool = RedisPoolCache()
            await self._aioredis_pool.populate()
            self._redis_conn = self._aioredis_pool._aioredis_pool
            self._logger.debug('Successfully initialized Redis pool')
        except Exception as e:
            self._logger.error(f'Failed to initialize Redis pool: {e}')
            raise
        
    async def _init_ipfs_client(self):
        """
        Initialize the IPFS client.

        This method creates a singleton instance of AsyncIPFSClientSingleton,
        initializes its sessions, and assigns the write and read clients to instance variables.
        """
        try:
            self._ipfs_singleton = AsyncIPFSClientSingleton(settings.ipfs)
            await self._ipfs_singleton.init_sessions()
            self._ipfs_writer_client = self._ipfs_singleton._ipfs_write_client
            self._ipfs_reader_client = self._ipfs_singleton._ipfs_read_client
            self._logger.debug('Successfully initialized IPFS client')
        except Exception as e:
            self._logger.error(f'Failed to initialize IPFS client: {e}')
            raise

    async def init_worker(self):
        """
        Initializes the worker by setting up all required connections and resources.
        
        This method initializes the Redis pool, RPC helper, protocol metadata,
        and starts the task cleanup process. It sets the _initialized flag to True
        when complete.
        """
        if not self._initialized:
            try:
                await self._init_redis_pool()
                self._logger.debug('Initialized Redis pool in CidCacher init_worker')
                
                await self._init_ipfs_client()
                self._logger.debug('Initialized IPFS client in CidCacher init_worker')
                
                self._initialized = True
                self._logger.info('CidCacher worker initialized successfully')
            except Exception as e:
                self._logger.error(f'Failed to initialize CidCacher worker: {e}')
                raise

    async def _cache_cids(self, cids: List[str]):
        """
        Cache CIDs in Redis.
        
        Args:
            cids (List[str]): List of CIDs to cache
        """
        if not cids:
            return

        try:

            # fetch existing cids cached from redis
            cid_data = await self._redis_conn.mget(
                [cid_cache(cid) for cid in cids]
            )
            existing_cids = [cid for cid, data in zip(cids, cid_data) if data is not None]
            cids = [cid for cid in cids if cid not in existing_cids]
            self._logger.info(f'Skipping {len(existing_cids)} CIDs that are already cached')

            tasks = [
                get_submission_data(cid, self._ipfs_reader_client, True)
                for cid in cids
            ]

            results = await asyncio.gather(
                *(asyncio.wait_for(task, timeout=10) for task in tasks),
                return_exceptions=True,
            )

            pipeline = self._redis_conn.pipeline()
            batch_cached_count = 0

            for cid, result in zip(cids, results):
                if isinstance(result, Exception):
                    self._logger.error(f'Error processing CID {cid}: {result}')
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

            if batch_cached_count > 0:
                self._logger.info(f'Successfully cached {batch_cached_count} out of {len(cids)} CIDs')
            elif len(cids) > 0:
                self._logger.warning(f'No CIDs were cached from {len(cids)} provided')

        except Exception as e:
            self._logger.error(f'Error caching CIDs: {e}')
            raise

    async def _periodic_cache_cids(self):
        """
        Periodically cache CIDs from Redis set.
        """
        self._logger.info('Starting periodic CID caching')
        
        while not self._shutdown_initiated:
            try:
                cids_to_cache = await self._redis_conn.spop(cids_to_cache_set(), count=200)
                self._logger.info(f'Popped {len(cids_to_cache)} CIDs from Redis set')
                # convert bytes to strings if necessary
                cids_to_cache = [cid.decode('utf-8') if isinstance(cid, bytes) else cid for cid in cids_to_cache]
                if cids_to_cache:
                    await self._cache_cids(cids_to_cache)
                else:
                    self._logger.info(f'No CIDs to cache, sleeping for {self._caching_interval} seconds')
                    await asyncio.sleep(self._caching_interval)
                
            except asyncio.CancelledError:
                self._logger.info("Periodic CID caching cancelled")
                break
            except Exception as e:
                self._logger.error(f'Error in periodic CID caching: {e}')
                await asyncio.sleep(self._caching_interval)

    async def _shutdown_gracefully(self):
        """
        Perform graceful shutdown of the CidCacher.
        """
        self._logger.info('Starting graceful shutdown')
        
        try:
            # Cancel all active tasks
            tasks_to_cancel = list(self._active_tasks)
            for _, task in tasks_to_cancel:
                if not task.done():
                    task.cancel()
            
            # Wait for tasks to complete or be cancelled
            if tasks_to_cancel:
                await asyncio.gather(*[task for _, task in tasks_to_cancel], return_exceptions=True)
                self._logger.info(f'Cancelled {len(tasks_to_cancel)} active tasks')
            
            # Close IPFS client sessions
            if self._ipfs_singleton:
                await self._ipfs_singleton.close_sessions()
                self._logger.info('Closed IPFS client sessions')
            
            # Close Redis connection
            if hasattr(self, '_redis_conn') and self._redis_conn:
                await self._redis_conn.close()
                self._logger.info('Closed Redis connection')
                
        except Exception as e:
            self._logger.error(f'Error during graceful shutdown: {e}')

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
            self._logger.info(f'Set file descriptor limit to {settings.rlimit.file_descriptors}')
            
            # Register signal handlers for graceful shutdown
            for signame in [SIGINT, SIGTERM, SIGQUIT]:
                signal(signame, self._signal_handler)
                
            # Use uvloop for better performance
            asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

            ev_loop = asyncio.get_event_loop()
            CidCacher._event_loop = ev_loop  # Store the event loop
            self._event_loop = ev_loop
            
            # Initialize worker
            ev_loop.run_until_complete(self.init_worker())
            
            # Start main caching task
            main_task = ev_loop.create_task(self._periodic_cache_cids())
            
            try:
                self._logger.info('CidCacher started successfully')
                # Run the event loop until shutdown is requested
                ev_loop.run_forever()
            except KeyboardInterrupt:
                self._logger.info('KeyboardInterrupt received, shutting down')
            finally:
                self._logger.info('Stopping event loop and cleaning up')
                
                # Cancel main task
                if not main_task.done():
                    main_task.cancel()
                
                # Perform graceful shutdown
                ev_loop.run_until_complete(self._shutdown_gracefully())
                
                # Close the event loop
                ev_loop.close()
                self._logger.info('CidCacher shutdown complete')
                
        except Exception as e:
            self._logger.error(f"Fatal error in CidCacher process: {e}")
            self._logger.error(traceback.format_exc())
            raise


if __name__ == '__main__':
    cacher = CidCacher('CidCacher')
    cacher.run()
