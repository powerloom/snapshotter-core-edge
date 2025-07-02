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
from snapshotter.utils.redis.redis_keys import agg_volume_pool_key
from snapshotter.utils.dramatiq_queues import TRADE_VOLUME_WORKER_QUEUE_NAME
from computes.api.utils.data_utils import get_uniswap_trade_volume_agg


class TradeVolumeWorker(multiprocessing.Process):
    def __init__(self, name, **kwargs):
        super(TradeVolumeWorker, self).__init__(name=name, **kwargs)
        self._logger = default_logger.bind(module="TradeVolumeWorker")
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

    @dramatiq.actor(queue_name=TRADE_VOLUME_WORKER_QUEUE_NAME)
    def process_volume_aggregation_actor(self, payload: Dict):
        try:
            self._logger.info(f"Processing trade volume aggregation for: {payload}")
            asyncio.run_coroutine_threadsafe(
                self._process_volume_aggregation_async(payload),
                self._event_loop,
            ).result(timeout=60)
            self._logger.info("Trade volume aggregation complete.")
        except Exception as e:
            self._logger.error(f"Error in trade volume aggregation actor: {e}", exc_info=True)

    async def _process_volume_aggregation_async(self, payload: Dict):
        await self.redis_pool.populate()
        redis_conn = self.redis_pool.get_client()
        task_type = payload.get('task_type')
        epoch_id = payload.get('epochId')
        project_id = payload.get('projectId')

        time_intervals = [86400, 604800] # 24h, 7d

        for interval in time_intervals:
            try:
                self._logger.info(f"Calculating trade volume for {project_id} over {interval} seconds.")
                trade_volume_agg = await get_uniswap_trade_volume_agg(
                    redis_conn=redis_conn,
                    project_id=project_id,
                    time_interval=interval,
                )

                if trade_volume_agg:
                    cache_key = agg_volume_pool_key(project_id, interval)
                    await redis_conn.set(cache_key, trade_volume_agg['totalTradeVolume'], ex=interval*2)
                    self._logger.info(f"Cached trade volume for {project_id} over {interval} seconds.")

            except Exception as e:
                self._logger.error(f"Error calculating trade volume for {project_id} over {interval} seconds: {e}", exc_info=True)

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

        worker = Worker(self.broker, queues=["trade_volume_worker_queue"])
        self._logger.info("Starting TradeVolumeWorker Dramatiq worker internal threads...")
        worker.start()

        try:
            self._logger.info("Running TradeVolumeWorker main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("TradeVolumeWorker main event loop stopped. Shutting down...")
            try:
                self._logger.info("Stopping TradeVolumeWorker Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("TradeVolumeWorker Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping TradeVolumeWorker Dramatiq worker: {e}", exc_info=True)
            
            self._logger.info("Closing TradeVolumeWorker event loop...")
            ev_loop.close()
            self._logger.info("TradeVolumeWorker Event loop closed.")


if __name__ == '__main__':
    worker = TradeVolumeWorker('TradeVolumeWorker')
    worker.run()