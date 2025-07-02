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
from computes.api.utils.data_utils import get_uniswap_price_series_agg, get_uniswap_v3_pool_trades
from snapshotter.utils.redis.redis_keys import base_snapshot_project_id, series_price_key, trades_snapshot_project_id, series_trades_key
from snapshotter.utils.dramatiq_queues import TIMESERIES_WORKER_QUEUE_NAME


class TimeSeriesWorker(multiprocessing.Process):
    def __init__(self, name, **kwargs):
        super(TimeSeriesWorker, self).__init__(name=name, **kwargs)
        self._logger = default_logger.bind(module="TimeSeriesWorker")
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

    @dramatiq.actor(queue_name=TIMESERIES_WORKER_QUEUE_NAME)
    def process_timeseries_aggregation_actor(self, payload: Dict):
        try:
            self._logger.info(f"Processing time series aggregation for: {payload}")
            asyncio.run_coroutine_threadsafe(
                self._process_timeseries_aggregation_async(payload),
                self._event_loop,
            ).result(timeout=60)
            self._logger.info("Time series aggregation complete.")
        except Exception as e:
            self._logger.error(f"Error in time series aggregation actor: {e}", exc_info=True)

    async def _process_timeseries_aggregation_async(self, payload: Dict):
        await self.redis_pool.populate()
        redis_conn = self.redis_pool.get_client()
        task_type = payload.get('task_type')
        epoch_id = payload.get('epochId')
        project_id = payload.get('projectId')

        # Example: Pre-compute price series for common intervals
        if task_type.startswith('baseSnapshot:'):
            pool_address = project_id.split(':')[1] # Extract pool_address from project_id
            # For now, we assume token_address is part of the project_id or can be derived
            # For a real implementation, you might need to fetch it or pass it in payload
            token_address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2" # Example WETH address

            if pool_address and token_address:
                time_intervals = [86400, 604800] # 24h, 7d
                step_seconds = 3600 # 1 hour step
                for interval in time_intervals:
                    try:
                        self._logger.info(f"Calculating price series for {pool_address}/{token_address} over {interval}s with {step_seconds}s step.")
                        price_series = await get_uniswap_price_series_agg(
                            redis_conn=redis_conn,
                            # rpc_helper, anchor_rpc_helper, ipfs_reader, protocol_state_contract - these need to be passed or initialized
                            time_interval=interval,
                            project_id=base_snapshot_project_id(pool_address),
                            token_address=token_address,
                            step_seconds=step_seconds,
                        )
                        if price_series:
                            cache_key = series_price_key(pool_address, token_address, interval, step_seconds)
                            await redis_conn.set(cache_key, json.dumps(price_series['priceSeries']), ex=interval*2)
                            self._logger.info(f"Cached price series for {pool_address}/{token_address}.")
                    except Exception as e:
                        self._logger.error(f"Error calculating price series for {pool_address}/{token_address}: {e}", exc_info=True)

        # Example: Pre-compute pool trades for common intervals
        if task_type.startswith('tradesSnapshot:'):
            pool_address = project_id.split(':')[1] # Extract pool_address from project_id
            if pool_address:
                end_timestamp = int(time.time())
                start_timestamp = end_timestamp - 86400 # Last 24 hours
                try:
                    self._logger.info(f"Calculating pool trades for {pool_address} from {start_timestamp} to {end_timestamp}.")
                    pool_trades = await get_uniswap_v3_pool_trades(
                        redis_conn=redis_conn,
                        # anchor_rpc_helper, rpc_helper, ipfs_reader, protocol_state_contract - these need to be passed or initialized
                        project_id=trades_snapshot_project_id(pool_address),
                        pool_address=pool_address,
                        start_timestamp=start_timestamp,
                        end_timestamp=end_timestamp,
                    )
                    if pool_trades:
                        cache_key = series_trades_key(pool_address, start_timestamp, end_timestamp)
                        await redis_conn.set(cache_key, json.dumps(pool_trades), ex=86400*2)
                        self._logger.info(f"Cached pool trades for {pool_address}.")
                except Exception as e:
                    self._logger.error(f"Error calculating pool trades for {pool_address}: {e}", exc_info=True)

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

        worker = Worker(self.broker, queues=["timeseries_worker_queue"])
        self._logger.info("Starting TimeSeriesWorker Dramatiq worker internal threads...")
        worker.start()

        try:
            self._logger.info("Running TimeSeriesWorker main event loop...")
            ev_loop.run_forever()
        finally:
            self._logger.info("TimeSeriesWorker main event loop stopped. Shutting down...")
            try:
                self._logger.info("Stopping TimeSeriesWorker Dramatiq worker internal threads...")
                worker.stop()
                self._logger.info("TimeSeriesWorker Dramatiq worker stopped.")
            except Exception as e:
                self._logger.error(f"Error stopping TimeSeriesWorker Dramatiq worker: {e}", exc_info=True)
            
            self._logger.info("Closing TimeSeriesWorker event loop...")
            ev_loop.close()
            self._logger.info("TimeSeriesWorker Event loop closed.")


if __name__ == '__main__':
    worker = TimeSeriesWorker('TimeSeriesWorker')
    worker.run()