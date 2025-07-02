import asyncio
import multiprocessing
import time
import traceback
from typing import Dict

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
    def __init__(self, name, **kwargs):
        super(MetadataWorker, self).__init__(name=name, **kwargs)
        self._logger = default_logger.bind(module="MetadataWorker")
        self._shutdown_initiated = False
        self.redis_pool = RedisPoolCache()
        self.broker = RedisBroker(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
        )
        self.broker.add_middleware(AsyncIO())
        dramatiq.set_broker(self.broker)

    def _signal_handler(self, signum, frame):
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._logger.info('Shutdown initiated')

    @dramatiq.actor(queue_name=METADATA_WORKER_QUEUE_NAME)
    def process_metadata_fetching_actor(self, payload: Dict):
        try:
            self._logger.info(f"Processing metadata fetching for: {payload}")
            asyncio.run_coroutine_threadsafe(
                self._process_metadata_fetching_async(payload),
                self._event_loop,
            ).result(timeout=60)
            self._logger.info("Metadata fetching complete.")
        except Exception as e:
            self._logger.error(f"Error in metadata fetching actor: {e}", exc_info=True)

    async def _process_metadata_fetching_async(self, payload: Dict):
        await self.redis_pool.populate()
        redis_conn = self.redis_pool.get_client()
        task_type = payload.get('task_type')
        epoch_id = payload.get('epochId')
        project_id = f"{task_type}:{settings.namespace}"

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
        for signame in [SIGINT, SIGTERM, SIGQUIT]:
            signal(signame, self._signal_handler)
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

        ev_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(ev_loop)
        self._event_loop = ev_loop

        for middleware in self.broker.middleware:
            if isinstance(middleware, dramatiq.middleware.AsyncIO):
                middleware.event_loop = ev_loop

        worker = Worker(self.broker, queues=["metadata_worker_queue"])
        self._logger.info("Starting MetadataWorker Dramatiq worker internal threads...")
        worker.start()

        try:
            self._logger.info("Running MetadataWorker main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("MetadataWorker main event loop stopped. Shutting down...")
            try:
                self._logger.info("Stopping MetadataWorker Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("MetadataWorker Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping MetadataWorker Dramatiq worker: {e}", exc_info=True)
            
            self._logger.info("Closing MetadataWorker event loop...")
            ev_loop.close()
            self._logger.info("MetadataWorker Event loop closed.")


if __name__ == '__main__':
    worker = MetadataWorker('MetadataWorker')
    worker.run()