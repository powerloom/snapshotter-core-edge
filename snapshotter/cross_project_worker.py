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
from computes.api.utils.data_utils import get_uniswap_v3_base_snapshots_for_token, get_uniswap_v3_all_trades_snapshot, get_uniswap_v3_token_prices_all_snapshot
from snapshotter.utils.redis.redis_keys import agg_snapshot_base_all_pools_key, agg_snapshot_all_trades_key, agg_snapshot_token_prices_key
from snapshotter.utils.dramatiq_queues import CROSS_PROJECT_WORKER_QUEUE_NAME


class CrossProjectWorker(multiprocessing.Process):
    def __init__(self, name, **kwargs):
        super(CrossProjectWorker, self).__init__(name=name, **kwargs)
        self._logger = default_logger.bind(module="CrossProjectWorker")
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

    @dramatiq.actor(queue_name=CROSS_PROJECT_WORKER_QUEUE_NAME)
    def process_cross_project_aggregation_actor(self, payload: Dict):
        try:
            self._logger.info(f"Processing cross-project aggregation for: {payload}")
            asyncio.run_coroutine_threadsafe(
                self._process_cross_project_aggregation_async(payload),
                self._event_loop,
            ).result(timeout=60)
            self._logger.info("Cross-project aggregation complete.")
        except Exception as e:
            self._logger.error(f"Error in cross-project aggregation actor: {e}", exc_info=True)

    async def _process_cross_project_aggregation_async(self, payload: Dict):
        await self.redis_pool.populate()
        redis_conn = self.redis_pool.get_client()
        task_type = payload.get('task_type')
        epoch_id = payload.get('epochId')
        project_id = payload.get('projectId')

        # Example: Aggregating base snapshots for all pools of a token
        if project_id.startswith('baseSnapshot:'):
            parts = project_id.split(':')
            if len(parts) >= 2:
                token_address = parts[1]
                try:
                    self._logger.info(f"Calculating base snapshots for all pools of token {token_address}.")
                    base_snapshots_all_pools = await get_uniswap_v3_base_snapshots_for_token(
                        redis_conn=redis_conn,
                        token_address=token_address,
                    )
                    if base_snapshots_all_pools:
                        cache_key = agg_snapshot_base_all_pools_key(token_address)
                        await redis_conn.set(cache_key, json.dumps(base_snapshots_all_pools), ex=86400*2)
                        self._logger.info(f"Cached base snapshots for all pools of token {token_address}.")
                except Exception as e:
                    self._logger.error(f"Error calculating base snapshots for all pools of token {token_address}: {e}", exc_info=True)

        # Example: Aggregating all trades snapshot
        if project_id.startswith('tradesSnapshot:'):
            try:
                self._logger.info(f"Calculating all trades snapshot.")
                all_trades_snapshot = await get_uniswap_v3_all_trades_snapshot(
                    redis_conn=redis_conn,
                )
                if all_trades_snapshot:
                    cache_key = agg_snapshot_all_trades_key()
                    await redis_conn.set(cache_key, json.dumps(all_trades_snapshot), ex=86400*2)
                    self._logger.info(f"Cached all trades snapshot.")
            except Exception as e:
                self._logger.error(f"Error calculating all trades snapshot: {e}", exc_info=True)

        # Example: Aggregating token prices from all pools
        if project_id.startswith('tokenPools:'):
            parts = project_id.split(':')
            if len(parts) >= 2:
                token_address = parts[1]
                try:
                    self._logger.info(f"Calculating token prices for all pools of token {token_address}.")
                    token_prices_all_pools = await get_uniswap_v3_token_prices_all_snapshot(
                        redis_conn=redis_conn,
                        token_address=token_address,
                    )
                    if token_prices_all_pools:
                        cache_key = agg_snapshot_token_prices_key(token_address)
                        await redis_conn.set(cache_key, json.dumps(token_prices_all_pools), ex=86400*2)
                        self._logger.info(f"Cached token prices for all pools of token {token_address}.")
                except Exception as e:
                    self._logger.error(f"Error calculating token prices for all pools of token {token_address}: {e}", exc_info=True)

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

        worker = Worker(self.broker, queues=["cross_project_worker_queue"])
        self._logger.info("Starting CrossProjectWorker Dramatiq worker internal threads...")
        worker.start()

        try:
            self._logger.info("Running CrossProjectWorker main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("CrossProjectWorker main event loop stopped. Shutting down...")
            try:
                self._logger.info("Stopping CrossProjectWorker Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("CrossProjectWorker Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping CrossProjectWorker Dramatiq worker: {e}", exc_info=True)
            
            self._logger.info("Closing CrossProjectWorker event loop...")
            ev_loop.close()
            self._logger.info("CrossProjectWorker Event loop closed.")


if __name__ == '__main__':
    worker = CrossProjectWorker('CrossProjectWorker')
    worker.run()