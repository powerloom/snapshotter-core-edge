import time
import logging
import asyncio
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch
from urllib.parse import urljoin

import pytest
from fakeredis import FakeAsyncRedis
from pytest_asyncio import fixture as async_fixture

from snapshotter.settings.config import settings
from snapshotter.utils.models.data_models import SnapshotterIssue
from snapshotter.utils.redis.redis_keys import callback_last_sent_by_issue
from snapshotter.utils.callback_helpers import send_telegram_notification_async
from snapshotter.utils.models.data_models import (
    TelegramEpochProcessingReportMessage,
    TelegramSnapshotterCoreReportMessage,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@async_fixture(scope='function')
async def mock_redis():
    """Fixture to provide a FakeAsyncRedis connection."""
    fake_redis = FakeAsyncRedis()
    yield fake_redis
    await fake_redis.flushdb()
    await fake_redis.close()


@async_fixture(scope='function')
async def mock_async_client():
    """Fixture to provide a mocked AsyncClient."""
    with patch('snapshotter.utils.callback_helpers.AsyncClient', autospec=True) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.post = AsyncMock()
        yield mock_client_instance


@async_fixture(scope='function')
async def mock_sync_client():
    """Fixture to provide a mocked SyncClient."""
    with patch('snapshotter.utils.callback_helpers.SyncClient', autospec=True) as MockClient:
        mock_client_instance = MockClient.return_value
        mock_client_instance.post = MagicMock()
        yield mock_client_instance

# --- Test Data --- 

SAMPLE_ISSUE = SnapshotterIssue(
    instanceID='test_instance',
    issueType='TEST_ERROR',
    projectID='test_project',
    epochId='123',
    timeOfReporting=str(time.time()),
    extra='Some extra info'
)

EPOCH_MESSAGE = TelegramEpochProcessingReportMessage(
    chatId='chat123',
    slotId=456,
    issue=SAMPLE_ISSUE
)

SNAPSHOTTER_MESSAGE = TelegramSnapshotterCoreReportMessage(
    chatId='chat123',
    slotId=456,
    issue=SAMPLE_ISSUE
)

# --- Tests for send_telegram_notification_async --- 

@pytest.mark.asyncio
async def test_send_telegram_async_disabled(mock_async_client, mock_redis, mocker):
    """Test that no notification is sent if Telegram reporting is disabled."""
    mocker.patch.object(settings.reporting, 'telegram_url', None)
    mocker.patch.object(settings.reporting, 'telegram_chat_id', None)

    await send_telegram_notification_async(mock_async_client, EPOCH_MESSAGE, mock_redis)

    mock_async_client.post.assert_not_called()
    mock_redis.get = AsyncMock(wraps=mock_redis.get)
    mock_redis.set = AsyncMock(wraps=mock_redis.set)
    await send_telegram_notification_async(mock_async_client, EPOCH_MESSAGE, mock_redis)
    mock_redis.get.assert_not_called()
    mock_redis.set.assert_not_called()
    logger.info(f"Test successful: {test_send_telegram_async_disabled.__name__}")

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message, expected_endpoint",
    [
        (EPOCH_MESSAGE, '/reportEpochProcessingIssue'),
        (SNAPSHOTTER_MESSAGE, '/reportSnapshotIssue'),
    ]
)
async def test_send_telegram_async_interval_disabled(message, expected_endpoint, mock_async_client, mock_redis, mocker):
    """Test sending when min_reporting_interval is 0 (disabled)."""
    mocker.patch.object(settings.reporting, 'telegram_url', 'http://fake-telegram.com')
    mocker.patch.object(settings.reporting, 'telegram_chat_id', 'chat123')
    mocker.patch.object(settings.reporting, 'min_reporting_interval', 0)

    mock_redis.get = AsyncMock(wraps=mock_redis.get)
    mock_redis.set = AsyncMock(wraps=mock_redis.set)

    futures = []
    original_create_task = asyncio.create_task

    def create_task_tracker(coro, *, name=None):
        logger.debug(f"create_task_tracker called with: {coro}")
        if asyncio.iscoroutine(coro):
             task = original_create_task(coro, name=name)
             logger.debug(f"  -> Tracking task: {task}")
             futures.append(task)
             return task
        else:
            logger.warning(f"  -> Not a coroutine: {coro}")
            raise TypeError("create_task requires a coroutine")

    patcher = patch('asyncio.create_task', side_effect=create_task_tracker)
    patcher.start()

    try:
        logger.info("Calling send_telegram_notification_async...")
        await send_telegram_notification_async(mock_async_client, message, mock_redis)
        logger.info("Finished send_telegram_notification_async call.")

        logger.info(f"Waiting for {len(futures)} captured future(s)...")
        if futures:
            done, pending = await asyncio.wait(futures, timeout=5)
            logger.info(f"Wait results: Done={len(done)}, Pending={len(pending)}")
            if pending:
                logger.error(f"Futures did not complete within timeout: {pending}")
            
            # Check for exceptions in completed tasks
            exceptions_found = []
            for task in done:
                try:
                    result = task.result()
                    logger.debug(f"Task {task.get_name()} completed with result: {result}")
                except Exception as task_exc:
                    logger.error(f"Exception occurred within awaited task {task.get_name()}: {task_exc}", exc_info=True)
                    exceptions_found.append(task_exc)
            
            # Fail test if exceptions occurred in tasks
            if exceptions_found:
                 raise AssertionError(f"Exceptions occurred in background tasks: {exceptions_found}") from exceptions_found[0]

        logger.info("Futures awaited (and checked for exceptions).")

    except Exception as e:
        logger.exception("Exception during test execution or wait")
        raise 
    finally:
        patcher.stop()
        logger.info("Patcher stopped.")

    try:
        mock_async_client.post.assert_awaited_once_with(
            url=urljoin(settings.reporting.telegram_url, expected_endpoint),
            json=message.dict(),
        )
        logger.info("Assert mock_async_client.post: PASSED")
    except AssertionError as e:
        logger.error(f"Assert mock_async_client.post: FAILED - {e}")
        raise

    # Check Redis calls were NOT made using call_count on the wrappers
    assert mock_redis.get.call_count == 0, "mock_redis.get should not have been called"
    logger.info("Check mock_redis.get.call_count == 0: PASSED")
    assert mock_redis.set.call_count == 0, "mock_redis.set should not have been called"
    logger.info("Check mock_redis.set.call_count == 0: PASSED")
    
    logger.info(f"Test successful: {test_send_telegram_async_interval_disabled.__name__} with params {message.issue.issueType}")

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message, expected_endpoint",
    [
        (EPOCH_MESSAGE, '/reportEpochProcessingIssue'),
        (SNAPSHOTTER_MESSAGE, '/reportSnapshotIssue'),
    ]
)
async def test_send_telegram_async_interval_enabled_first_time(message, expected_endpoint, mock_async_client, mock_redis, mocker):
    """Test sending when interval is enabled and it's the first time."""
    logger.info(f"Starting test: {test_send_telegram_async_interval_enabled_first_time.__name__} with {message.issue.issueType}")
    mocker.patch.object(settings.reporting, 'telegram_url', 'http://fake-telegram.com')
    mocker.patch.object(settings.reporting, 'telegram_chat_id', 'chat123')
    mocker.patch.object(settings.reporting, 'min_reporting_interval', 60) # 1 minute

    redis_key = callback_last_sent_by_issue(message.issue.issueType)
    await mock_redis.delete(redis_key)

    futures = []
    original_create_task = asyncio.create_task

    def create_task_tracker(coro, *, name=None):
        logger.debug(f"create_task_tracker called with: {coro}")
        if asyncio.iscoroutine(coro):
             task = original_create_task(coro, name=name)
             logger.debug(f"  -> Tracking task: {task}")
             futures.append(task)
             return task
        else:
            logger.warning(f"  -> Not a coroutine: {coro}")
            raise TypeError("create_task requires a coroutine")

    patcher = patch('asyncio.create_task', side_effect=create_task_tracker)
    patcher.start()

    try:
        logger.info("Calling send_telegram_notification_async...")
        await send_telegram_notification_async(mock_async_client, message, mock_redis)
        logger.info("Finished send_telegram_notification_async call.")

        logger.info(f"Waiting for {len(futures)} captured future(s)...")
        if futures:
            done, pending = await asyncio.wait(futures, timeout=5)
            logger.info(f"Wait results: Done={len(done)}, Pending={len(pending)}")
            if pending:
                logger.error(f"Futures did not complete within timeout: {pending}")

            exceptions_found = []
            for task in done:
                try:
                    result = task.result() # This will raise if the task had an exception
                    logger.debug(f"Task {task.get_name()} completed with result: {result}")
                except Exception as task_exc:
                    logger.error(f"Exception occurred within awaited task {task.get_name()}: {task_exc}", exc_info=True)
                    exceptions_found.append(task_exc)

        logger.info("Futures awaited (and checked for exceptions).")

    except Exception as e:
        logger.exception("Exception during test execution or gather")
        raise
    finally:
        patcher.stop()
        logger.info("Patcher stopped.")


    # Assertions
    logger.info("Running assertions...")
    assert await mock_redis.exists(redis_key), f"Redis key {redis_key} should have been set but was not found."
    logger.info("Check mock_redis.exists(key): PASSED")

    try:
        logger.info(f"mock_async_client.post await_count: {mock_async_client.post.await_count}")
        mock_async_client.post.assert_awaited_once_with(
            url=urljoin(settings.reporting.telegram_url, expected_endpoint),
            json=message.dict(),
        )
        logger.info("Assert mock_async_client.post: PASSED")
    except AssertionError as e:
        logger.error(f"Assert mock_async_client.post: FAILED - {e}")
        raise

    logger.info(f"Test successful: {test_send_telegram_async_interval_enabled_first_time.__name__} with params {message.issue.issueType}")

@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        EPOCH_MESSAGE,
        SNAPSHOTTER_MESSAGE,
    ]
)
async def test_send_telegram_async_interval_enabled_recently_sent(message, mock_async_client, mock_redis, mocker):
    """Test sending is skipped when interval is enabled and recently sent."""
    mocker.patch.object(settings.reporting, 'telegram_url', 'http://fake-telegram.com')
    mocker.patch.object(settings.reporting, 'telegram_chat_id', 'chat123')
    mocker.patch.object(settings.reporting, 'min_reporting_interval', 60) # 1 minute

    redis_key = callback_last_sent_by_issue(message.issue.issueType)
    await mock_redis.set(redis_key, str(time.time()), ex=settings.reporting.min_reporting_interval)

    mock_redis.get = AsyncMock(wraps=mock_redis.get)
    mock_redis.set = AsyncMock(wraps=mock_redis.set)

    await send_telegram_notification_async(mock_async_client, message, mock_redis)

    await asyncio.sleep(0)

    mock_redis.get.assert_awaited_once_with(redis_key)

    mock_async_client.post.assert_not_called()

    mock_redis.set.assert_not_called()
    logger.info(f"Test successful: {test_send_telegram_async_interval_enabled_recently_sent.__name__} with params {message.issue.issueType}")
