import asyncio
import json
import multiprocessing
import resource
import signal
import sys
import time
from functools import wraps
from signal import SIGINT
from signal import SIGQUIT
from signal import SIGTERM
from socket import gethostname
from typing import Union

import dramatiq
from dramatiq.brokers.redis import RedisBroker
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from redis import asyncio as aioredis
from web3 import Web3

from snapshotter.settings.config import settings
from snapshotter.utils.callback_helpers import send_telegram_notification_async
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.exceptions import GenericExitOnSignal
from snapshotter.utils.file_utils import read_json_file
from snapshotter.utils.models.data_models import EpochReleasedEvent
from snapshotter.utils.models.data_models import SnapshotBatchSubmittedEvent
from snapshotter.utils.models.data_models import SnapshotFinalizedEvent
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.models.data_models import SnapshotterReportState
from snapshotter.utils.models.data_models import TelegramEpochProcessingReportMessage
from snapshotter.utils.redis.redis_conn import RedisPoolCache
from snapshotter.utils.redis.redis_keys import event_detector_last_processed_block
from snapshotter.utils.redis.redis_keys import last_epoch_detected_epoch_id_key
from snapshotter.utils.redis.redis_keys import last_epoch_detected_timestamp_key
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key
from snapshotter.utils.rpc import get_event_sig_and_abi
from snapshotter.utils.rpc import RpcHelper


# Setup Dramatiq with Redis broker for sending only
redis_broker = RedisBroker(host=settings.redis.host, port=settings.redis.port)
dramatiq.set_broker(redis_broker)


def redis_cleanup(fn):
    """
    A decorator function that wraps the given function and handles cleanup of Redis connections in case of
    a GenericExitOnSignal or KeyboardInterrupt exception.

    Args:
        fn: The function to be wrapped.

    Returns:
        The wrapped function.
    """
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            fn(self, *args, **kwargs)
        except (GenericExitOnSignal, KeyboardInterrupt):
            try:
                if self._last_processed_block:
                    self._logger.debug(
                        'Saving last processed epoch to redis...',
                    )
                    self.ev_loop.run_until_complete(
                        self._redis_conn.set(
                            event_detector_last_processed_block,
                            json.dumps(self._last_processed_block),
                        ),
                    )
            except Exception as E:
                self._logger.opt(exception=settings.logs.debug_mode).error(
                    'Error while saving progress: {}', E,
                )
        except Exception as E:
            self._logger.opt(exception=settings.logs.debug_mode).error('Error while running: {}', E)
        finally:
            self._logger.debug('Shutting down!')
            sys.exit(0)

    return wrapper


class EventDetectorProcess(multiprocessing.Process):
    """
    A class for detecting system events and sending them using Dramatiq with Redis.

    Attributes:
        _redis_conn (aioredis.Redis): The Redis connection.
        _redis_pool (RedisPoolCache): The Redis connection pool.
    """

    _redis_conn: aioredis.Redis
    _redis_pool: RedisPoolCache
    _telegram_httpx_client: AsyncClient

    def __init__(self, name, **kwargs):
        """
        Initializes the EventDetectorProcess class.

        Args:
            name (str): The name of the process.
            **kwargs: Additional keyword arguments to be passed to the multiprocessing.Process class.
        """
        multiprocessing.Process.__init__(self, name=name, **kwargs)
        self._shutdown_initiated = False

        self._namespace = settings.namespace
        self._instance_id = settings.instance_id
        self.queue_name = (
            f'powerloom-event-detector_{settings.namespace}_{settings.instance_id}'
        )

        self._last_processed_block = None
        self._source_rpc_helper = RpcHelper(rpc_settings=settings.rpc)
        self._anchor_rpc_helper = RpcHelper(rpc_settings=settings.anchor_chain_rpc)
        self.contract_abi = None
        self._logger = None
        self.contract_address = settings.protocol_state.address

        self._simulation_completed = False
        self._hostname = gethostname()
        self._health_report_interval = settings.health_report_interval
        self._last_health_report_timestamp = 0

        # Initialize reporting and notification related attributes
        self._telegram_httpx_client = None
        self.notification_cooldown = settings.reporting.min_reporting_interval
        self.last_notification_time = 0

    async def _wait_for_simulation_completion(self):
        """
        Waits for the simulation to complete by sleeping for 15 seconds at a time.
        """
        while True:
            self._logger.info('Waiting for simulation completion...')
            if int(time.time()) - self._last_health_report_timestamp >= self._health_report_interval:
                await self.report_health_status()
            await asyncio.sleep(30)

    async def init(self):
        """
        Initializes the EventDetectorProcess by waiting for local collector initialization,
        broadcasting simulation submission, and waiting for simulation completion.
        """
        self._logger.debug(
            'Initializing SystemEventDetector. Awaiting local collector initialization and '
            'bootstrapping for 15 seconds...',
        )
        await asyncio.sleep(15)
        await self._broadcast_simulation_submission()
        # sleep until simulation is completed
        try:
            await asyncio.wait_for(self._wait_for_simulation_completion(), timeout=120)
        except asyncio.TimeoutError:
            pass  # expected
        except Exception as e:
            self._logger.error('Error while waiting for simulation completion: {}', e)
        finally:
            self._logger.debug('Breaking out of simulation completion wait loop...')
        # TODO: move simulation completion check to a separate cache entry or check on a contract entry
        # poll for simulation completion
        self._simulation_completed = True

    async def _broadcast_simulation_submission(self):
        """
        Prepares and broadcasts the simulation submission event.
        """
        self._logger.debug('⏳Preparing simulation submission...')
        current_block_number = await self._source_rpc_helper.get_current_block_number()

        event = EpochReleasedEvent(
            begin=current_block_number - 9,
            end=current_block_number,
            epochId=0,
            timestamp=int(time.time()),
        )

        self._logger.info(
            'Broadcasting simulation submission: {}', event,
        )
        self._broadcast_event('EpochReleased', event)

    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool if it hasn't been initialized yet.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool

    async def get_events(self, from_block: int, to_block: int):
        """
        Retrieves events from the blockchain for the given block range and returns them as a list of tuples.
        Each tuple contains the event name and an object representing the event data.

        Args:
            from_block (int): The starting block number.
            to_block (int): The ending block number.

        Returns:
            List[Tuple[str, Any]]: A list of tuples, where each tuple contains the event name
            and an object representing the event data.
        """
        events_log = await self._anchor_rpc_helper.get_events_logs(
            contract_address=self.contract_address,
            to_block=to_block,
            from_block=from_block,
            topics=[self.event_sig],
            event_abi=self.event_abi,
        )

        events = []
        new_epoch_detected = False
        latest_epoch_id = - 1
        for log in events_log:
            if log.args.dataMarketAddress != settings.data_market:
                self._logger.debug('Skipping event: {}', log)
                continue

            if log.event == 'EpochReleased':
                event = EpochReleasedEvent(
                    begin=log.args.begin,
                    end=log.args.end,
                    epochId=log.args.epochId,
                    timestamp=log.args.timestamp,
                )
                new_epoch_detected = True
                latest_epoch_id = max(latest_epoch_id, log.args.epochId)
                events.append((log.event, event))

            elif log.event == 'SnapshotBatchSubmitted':
                event = SnapshotBatchSubmittedEvent(
                    epochId=log.args.epochId,
                    batchCid=log.args.batchCid,
                    timestamp=log.args.timestamp,
                    transactionHash=log.transactionHash.hex(),
                )
                events.append((log.event, event))

            elif log.event == 'SnapshotFinalized':
                event = SnapshotFinalizedEvent(
                    epochId=log.args.epochId,
                    epochEnd=log.args.epochEnd,
                    projectId=log.args.projectId,
                    snapshotCid=log.args.snapshotCid,
                    timestamp=log.args.timestamp,
                )
                events.append((log.event, event))

        if new_epoch_detected:
            await self._redis_conn.set(
                last_epoch_detected_timestamp_key(),
                int(time.time()),
            )
            await self._redis_conn.set(
                last_epoch_detected_epoch_id_key(),
                latest_epoch_id,
            )
        if events:
            self._logger.info('Events detected in block range on Prost network {}-{}: {}', from_block, to_block, events)
        else:
            self._logger.debug('No events detected in block range on Prost network {}-{}', from_block, to_block)
        return events

    def _generic_exit_handler(self, signum, sigframe):
        """
        Handles the generic exit signal and initiates shutdown.

        Args:
            signum (int): The signal number.
            sigframe (object): The signal frame.

        Raises:
            GenericExitOnSignal: If the shutdown is initiated.
        """
        if (
            signum in [SIGINT, SIGTERM, SIGQUIT] and
            not self._shutdown_initiated
        ):
            self._shutdown_initiated = True
            raise GenericExitOnSignal

    def _broadcast_event(
        self,
        event_type: str,
        event: Union[EpochReleasedEvent, SnapshotFinalizedEvent, SnapshotBatchSubmittedEvent],
    ):
        """
        Broadcasts the given event using Dramatiq.

        Args:
            event_type (str): The type of the event being broadcasted.
            event (EventBase): The event being broadcasted.
        """
        if not self._simulation_completed and event.epochId != 0:
            self._logger.debug(
                'Skipping event broadcast for epoch {} as simulation is not complete. '
                'Incoming event: {}',
                event.epochId,
                event,
            )
            return
        self._logger.debug('Broadcasting event: {}', event)

        dramatiq.broker.get_broker().enqueue(
            dramatiq.Message(
                queue_name=self.queue_name,
                actor_name='handleEvent',  # Match actor name with event_receiver.py
                args=(event_type, event.json()),
                kwargs={},
                options={},
            ),
        )

    async def _detect_events(self):
        """
        Continuously detects events by fetching the current block and comparing it to the last processed block.
        If the last processed block is too far behind the current block, it processes the current block and
        broadcasts the events. The last processed block is saved in Redis for future reference.
        """
        while True:
            try:
                current_block = await self._anchor_rpc_helper.get_current_block()
                self._logger.debug('Current block: {}', current_block)

            except Exception as e:
                self._logger.opt(exception=settings.logs.debug_mode).error(
                    (
                        'Unable to fetch current block, ERROR: {}, '
                        'sleeping for {} seconds.'
                    ),
                    e,
                    settings.rpc.polling_interval,
                )

                await self._send_telegram_epoch_processing_notification(
                    error=e,
                )

                await asyncio.sleep(settings.rpc.polling_interval)
                continue

            # Only use redis if state is not locally present
            if not self._last_processed_block:
                last_processed_block_data = await self._redis_conn.get(
                    event_detector_last_processed_block,
                )

                if last_processed_block_data:
                    self._last_processed_block = json.loads(
                        last_processed_block_data,
                    )

            if self._last_processed_block == current_block:
                self._logger.debug(
                    'No new blocks detected, sleeping for {} seconds...',
                    settings.rpc.polling_interval,
                )
                await asyncio.sleep(settings.rpc.polling_interval)
                continue

            if self._last_processed_block:
                if current_block - self._last_processed_block >= 10:
                    self._logger.warning(
                        'Last processed block is too far behind current block, '
                        'processing current block',
                    )
                    self._last_processed_block = current_block - 10

                # Get events from current block to last_processed_block
                try:
                    events = await self.get_events(self._last_processed_block + 1, current_block)
                except Exception as e:
                    self._logger.opt(exception=settings.logs.debug_mode).error(
                        (
                            'Unable to fetch events from block {} to block {}, '
                            'ERROR: {}, sleeping for {} seconds.'
                        ),
                        self._last_processed_block + 1,
                        current_block,
                        e,
                        settings.rpc.polling_interval,
                    )

                    await self._send_telegram_epoch_processing_notification(
                        error=e,
                    )

                    await asyncio.sleep(settings.rpc.polling_interval)
                    continue

            else:
                self._logger.debug(
                    'No last processed epoch found, processing current block',
                )

                try:
                    events = await self.get_events(current_block, current_block)
                except Exception as e:
                    self._logger.opt(exception=settings.logs.debug_mode).error(
                        (
                            'Unable to fetch events from block {} to block {}, '
                            'ERROR: {}, sleeping for {} seconds.'
                        ),
                        current_block,
                        current_block,
                        e,
                        settings.rpc.polling_interval,
                    )

                    await self._send_telegram_epoch_processing_notification(
                        error=e,
                    )

                    await asyncio.sleep(settings.rpc.polling_interval)
                    continue

            for event_type, event in events:
                self._logger.debug(
                    'Processing event: {}', event,
                )
                self._broadcast_event(event_type, event)

            self._last_processed_block = current_block

            await self._redis_conn.set(event_detector_last_processed_block, json.dumps(current_block))
            self._logger.debug(
                'DONE: Processed blocks till, saving in redis: {}',
                current_block,
            )
            self._logger.debug(
                'Sleeping for {} seconds...',
                settings.rpc.polling_interval,
            )

            if int(time.time()) - self._last_health_report_timestamp >= self._health_report_interval:
                await self.report_health_status()

            await asyncio.sleep(settings.rpc.polling_interval)

    async def _init_rpc(self):
        """
        Initializes the RpcHelper instances for both anchor and source chains.
        """
        await self._anchor_rpc_helper.init()
        await self._source_rpc_helper.init()

    async def _init_httpx_client(self):
        """
        Initializes the httpx client for sending Telegram notifications.
        """
        # Initialize HTTP client for Telegram notifications
        self._telegram_httpx_client = AsyncClient(
            base_url=settings.reporting.telegram_url,
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=AsyncHTTPTransport(
                limits=Limits(
                    max_connections=100,
                    max_keepalive_connections=50,
                    keepalive_expiry=None,
                ),
            ),
        )

    async def _send_telegram_epoch_processing_notification(
        self,
        error: Exception,
    ):
        """
        Send a Telegram notification about epoch processing errors.

        This method constructs and sends a detailed error notification via Telegram
        when epoch processing encounters issues. The notification includes instance
        details, error information, and current status.

        Args:
            error (Exception): The error that occurred during processing

        Raises:
            Various exceptions possible during HTTP requests
        """

        if (int(time.time()) - self.last_notification_time) >= self.notification_cooldown and \
            (settings.reporting.telegram_url and settings.reporting.telegram_chat_id):

            if not self._telegram_httpx_client:
                self._logger.error('Telegram client not initialized')
                return

            try:
                telegram_message = TelegramEpochProcessingReportMessage(
                    chatId=settings.reporting.telegram_chat_id,
                    slotId=settings.slot_id,
                    issue=SnapshotterIssue(
                        instanceID=settings.instance_id,
                        issueType=SnapshotterReportState.UNHEALTHY_EPOCH_PROCESSING.value,
                        projectID='',
                        epochId='',
                        timeOfReporting=str(time.time()),
                        extra=json.dumps({'issueDetails': f'Error : {error}'}),
                    ),
                )

                await send_telegram_notification_async(
                    client=self._telegram_httpx_client,
                    message=telegram_message,
                    redis_conn=self._redis_conn,
                )

                self.last_notification_time = int(time.time())
            except Exception as e:
                self._logger.error('Error sending Telegram notification: {}', e)

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
            self._last_health_report_timestamp = int(time.time())
            self._logger.debug(f'Reported health for {self._hostname} at {current_timestamp}')
        except Exception as e:
            self._logger.error(f'Failed to report health status for hostname {self._hostname}: {e}')

    @redis_cleanup
    def run(self):
        """
        Starts the event detection process.

        This method initializes the necessary components, sets up signal handlers,
        initializes RPC connections, and begins the event detection loop.
        """
        # Initialize the event loop
        self.ev_loop = asyncio.get_event_loop()

        self._logger = default_logger.bind(
            module='SystemEventDetector',
        )
        self.contract_abi = read_json_file(
            settings.protocol_state.abi,
            self._logger,
        )
        # Initialize the Redis pool
        self.ev_loop.run_until_complete(self._init_redis_pool())

        # Set resource limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Set up signal handlers
        for signame in [signal.SIGINT, signal.SIGTERM, signal.SIGQUIT]:
            signal.signal(signame, self._generic_exit_handler)

        # Initialize RPC connections
        self.ev_loop.run_until_complete(self._init_rpc())

        # Initialize the event detector
        self.ev_loop.run_until_complete(self.init())

        # Initialize the httpx client
        self.ev_loop.run_until_complete(self._init_httpx_client())

        # Check if simulation is completed
        if not self._simulation_completed:
            self._logger.info('Simulation not completed after polling period. Exiting...')
            sys.exit(1)

        # Set up the contract and event ABIs
        self.contract = self._anchor_rpc_helper.get_current_node()['web3_client'].eth.contract(
            address=Web3.to_checksum_address(self.contract_address),
            abi=self.contract_abi,
        )

        EVENTS_ABI = {
            'EpochReleased': self.contract.events.EpochReleased._get_event_abi(),
            'DayStartedEvent': self.contract.events.DayStartedEvent._get_event_abi(),
            'SnapshotFinalized': self.contract.events.SnapshotFinalized._get_event_abi(),
            'SnapshotBatchSubmitted': self.contract.events.SnapshotBatchSubmitted._get_event_abi(),
        }
        self.event_sig, self.event_abi = get_event_sig_and_abi(
            {
                'EpochReleased': 'EpochReleased(address,uint256,uint256,uint256,uint256)',
                'DayStartedEvent': 'DayStartedEvent(address,uint256,uint256)',
                'SnapshotFinalized': 'SnapshotFinalized(address,uint256,uint256,string,string,uint256)',
                'SnapshotBatchSubmitted': 'SnapshotBatchSubmitted(address,string,uint256,uint256)',
            },
            EVENTS_ABI,
        )

        # Start the event detection loop
        self.ev_loop.run_until_complete(self._detect_events())


if __name__ == '__main__':
    event_detector = EventDetectorProcess('SystemEventDetector')
    event_detector.run()
