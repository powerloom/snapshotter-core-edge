import asyncio
import functools
from abc import ABC
from abc import ABCMeta
from abc import abstractmethod
from typing import Union
from urllib.parse import urljoin

from httpx import AsyncClient
from httpx import Client as SyncClient
from ipfs_client.main import AsyncIPFSClient
from pydantic import BaseModel
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.default_logger import default_logger
from snapshotter.utils.models.data_models import TelegramEpochProcessingReportMessage
from snapshotter.utils.models.data_models import TelegramSnapshotterCoreReportMessage
from snapshotter.utils.models.message_models import EpochBase
from snapshotter.utils.models.message_models import PowerloomCalculateAggregateMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotProcessMessage
from snapshotter.utils.models.message_models import PowerloomSnapshotSubmittedMessage
from snapshotter.utils.redis.redis_keys import callback_last_sent_by_issue
from snapshotter.utils.rpc import RpcHelper

# Setup logger for this module
helper_logger = default_logger.bind(module='Callback|Helpers')


def misc_notification_callback_result_handler(fut: asyncio.Task):
    """
    Handles the result of a callback or notification task.

    Args:
        fut (asyncio.Task): The task object representing the callback or notification.

    Returns:
        None
    """
    try:
        r = fut.result()
    except Exception as e:
        # Log the exception with full traceback if debug_mode is True
        if settings.logs.debug_mode:
            helper_logger.opt(exception=settings.logs.debug_mode).error(
                'Exception while sending callback or notification: {}', e,
            )
        else:
            helper_logger.error('Exception while sending callback or notification: {}', e)
    else:
        helper_logger.debug('Callback or notification result:{}', r)


def sync_notification_callback_result_handler(f: functools.partial):
    """
    Handles the result of a synchronous notification callback.

    Args:
        f (functools.partial): The function to handle.

    Returns:
        None
    """
    try:
        result = f()
    except Exception as exc:
        # Log the exception with full traceback if debug_mode is True
        if settings.logs.debug_mode:
            helper_logger.opt(exception=settings.logs.debug_mode).error(
                'Exception while sending callback or notification: {}', exc,
            )
        else:
            helper_logger.error('Exception while sending callback or notification: {}', exc)
    else:
        helper_logger.debug('Callback or notification result:{}', result)


async def send_telegram_notification_async(
    client: AsyncClient,
    message: Union[TelegramEpochProcessingReportMessage, TelegramSnapshotterCoreReportMessage],
    redis_conn: aioredis.Redis,
):
    """
    Sends an asynchronous Telegram notification for reporting issues.

    This function checks if Telegram reporting is configured, checks the minimum reporting interval via Redis,
    and then sends the appropriate message based on its type (epoch processing issue or snapshotter issue).

    Args:
        client (AsyncClient): The async HTTP client to use for sending notifications.
        message (Union[TelegramEpochProcessingReportMessage, TelegramSnapshotterCoreReportMessage]): The message to send as a Telegram notification.
        redis_conn (aioredis.Redis): Redis connection for rate limiting.

    Returns:
        None
    """

    if not settings.reporting.telegram_url or not settings.reporting.telegram_chat_id:
        return

    # Check if the last notification was sent within the minimum reporting interval
    issue_type = None
    time_of_reporting = None
    if isinstance(message, TelegramEpochProcessingReportMessage) or isinstance(message, TelegramSnapshotterCoreReportMessage):
        issue_type = message.issue.issueType
        time_of_reporting = message.issue.timeOfReporting

    if issue_type and time_of_reporting and settings.reporting.min_reporting_interval > 0:
        last_sent_timestamp = await redis_conn.get(
            callback_last_sent_by_issue(issue_type),
        )
        if last_sent_timestamp:
            helper_logger.debug(
                'Not sending Telegram notification for {} because the last notification was sent within the minimum reporting interval',
                issue_type,
            )
            return
        else:
            # Set the timestamp for the current notification if not found
            # We don't await this specifically, let it run in the background
            asyncio.create_task(
                redis_conn.set(
                    name=callback_last_sent_by_issue(issue_type),
                    value=time_of_reporting,
                    ex=settings.reporting.min_reporting_interval,
                ),
            )

    if isinstance(message, TelegramEpochProcessingReportMessage):
        endpoint = '/reportEpochProcessingIssue'
    elif isinstance(message, TelegramSnapshotterCoreReportMessage):
        endpoint = '/reportSnapshotterCoreIssue'
    else:
        helper_logger.error(
            f'Unsupported telegram message type: {type(message)} - message not sent',
        )
        return

    f = asyncio.create_task(
        client.post(
            url=urljoin(settings.reporting.telegram_url, endpoint),
            json=message.dict(),
        ),
    )
    f.add_done_callback(misc_notification_callback_result_handler)


def send_telegram_notification_sync(
    client: SyncClient,
    message: Union[TelegramEpochProcessingReportMessage, TelegramSnapshotterCoreReportMessage],
):
    """
    Sends a synchronous Telegram notification for reporting issues using the sync_notification_callback_result_handler.

    This function checks if Telegram reporting is configured, determines the correct endpoint,
    and then delegates the actual HTTP POST call and result/exception handling to
    sync_notification_callback_result_handler.
    Rate limiting is NOT handled by this function and should be implemented by the caller if needed.

    Args:
        client (SyncClient): The sync HTTP client to use for sending notifications.
        message (Union[TelegramEpochProcessingReportMessage, TelegramSnapshotterCoreReportMessage]): The message to send as a Telegram notification.

    Returns:
        None
    """

    if not settings.reporting.telegram_url or not settings.reporting.telegram_chat_id:
        helper_logger.debug('Telegram reporting not configured, skipping notification.')
        return

    if isinstance(message, TelegramEpochProcessingReportMessage):
        endpoint = '/reportEpochProcessingIssue'
    elif isinstance(message, TelegramSnapshotterCoreReportMessage):
        endpoint = '/reportSnapshotterCoreIssue'
    else:
        helper_logger.error(
            f'Unsupported telegram message type: {type(message)} - message not sent',
        )
        return

    f = functools.partial(
        client.post,
        url=urljoin(settings.reporting.telegram_url, endpoint),
        json=message.dict(),
    )

    sync_notification_callback_result_handler(f)

class GenericProcessorSnapshot(ABC):
    """
    Abstract base class for snapshot processors.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        epoch: PowerloomSnapshotProcessMessage,
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Abstract method to compute the snapshot.

        Args:
            epoch (PowerloomSnapshotProcessMessage): The epoch message.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
        """
        pass


class GenericPreloader(ABC):
    """
    Abstract base class for preloaders.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        epoch: EpochBase,
        redis_conn: aioredis.Redis,
        rpc_helper: RpcHelper,
    ):
        """
        Abstract method to compute preload data.

        Args:
            epoch (EpochBase): The epoch message.
            redis_conn (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
        """
        pass

    @abstractmethod
    async def cleanup(self):
        """
        Abstract method to clean up resources.
        """
        pass


class GenericProcessorAggregate(ABC):
    """
    Abstract base class for aggregate processors.
    """
    __metaclass__ = ABCMeta

    def __init__(self):
        pass

    @abstractmethod
    async def compute(
        self,
        msg_obj: Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage],
        redis: aioredis.Redis,
        rpc_helper: RpcHelper,
        anchor_rpc_helper: RpcHelper,
        ipfs_reader: AsyncIPFSClient,
        protocol_state_contract,
        project_id: str,
    ):
        """
        Abstract method to compute aggregate processing.

        Args:
            msg_obj (Union[PowerloomSnapshotSubmittedMessage, PowerloomCalculateAggregateMessage]): The message object.
            redis (aioredis.Redis): Redis connection.
            rpc_helper (RpcHelper): RPC helper instance.
            anchor_rpc_helper (RpcHelper): Anchor RPC helper instance.
            ipfs_reader (AsyncIPFSClient): IPFS reader instance.
            protocol_state_contract: Protocol state contract.
            project_id (str): Project ID.
        """
        pass


class PreloaderAsyncFutureDetails(BaseModel):
    """
    Pydantic model for preloader async future details.
    """
    obj: GenericPreloader
    future: asyncio.Task

    class Config:
        arbitrary_types_allowed = True
