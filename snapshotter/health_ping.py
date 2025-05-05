import asyncio
import dramatiq
import socket
import sys
import time
import uvloop

from dramatiq.brokers.redis import RedisBroker
from loguru import logger
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from pydantic import BaseModel
from redis import asyncio as aioredis

from snapshotter.settings.config import settings
from snapshotter.utils.redis.redis_keys import service_health_timestamps_key


# TODO: flag redis active_status_key as unhealthy if healthcheck fails


async def send_failure_notifications(client: AsyncClient, message: BaseModel):
    """
    Send failure notifications to a Slack channel.

    Args:
        client (AsyncClient): An asynchronous HTTP client.
        message (BaseModel): A Pydantic model containing the notification message.

    Returns:
        None
    """
    if settings.reporting.telegram_url:
        await client.post(
            url=settings.reporting.telegram_url,
            json=message.dict(),
        )



REDIS_HOST = settings.redis.host
REDIS_PORT = settings.redis.port
REDIS_PASSWORD = settings.redis.password
MAX_AGE_SECONDS = 300



async def check_health(hostname_to_check: str) -> bool:
    """Checks the health of a service based on its last reported timestamp in Redis."""
    redis_conn = None
    try:
        redis_conn = await aioredis.from_url(
            f'redis://{REDIS_HOST}:{REDIS_PORT}',
            password=REDIS_PASSWORD,
            decode_responses=False,  # Read bytes first
        )

        timestamp_bytes = await redis_conn.hget(
            service_health_timestamps_key(),
            hostname_to_check,
        )

        if timestamp_bytes is None:
            print(f'ERROR: No health timestamp found for {hostname_to_check}', file=sys.stderr)
            return False

        last_seen_timestamp = int(timestamp_bytes.decode())
        current_time = int(time.time())
        age = current_time - last_seen_timestamp

        if age <= MAX_AGE_SECONDS:
            print(f'OK: {hostname_to_check} last seen {age}s ago.', file=sys.stderr)
            return True
        else:
            print(f'ERROR: {hostname_to_check} last seen {age}s ago (older than {MAX_AGE_SECONDS}s).', file=sys.stderr)
            return False

    except ConnectionRefusedError:
        print(f'ERROR: Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}', file=sys.stderr)
        return False
    except Exception as e:
        print(f'ERROR: Health check failed for {hostname_to_check}: {e}', file=sys.stderr)
        return False
    finally:
        if redis_conn:
            await redis_conn.close()


def update_health_timestamp_with_client(hostname: str, redis_broker: RedisBroker, logger: logger):
    """Generic function executed by Dramatiq actors to update the health timestamp.
    
    Accepts a Dramatiq broker instance and accesses its sync Redis client.
    """
    if not hostname:
        logger.error("Received health ping request without a hostname.")
        return
    if redis_broker is None:
        logger.error(f"Received None redis_broker in health update for {hostname}")
        return
    if not hasattr(redis_broker, 'client') or redis_broker.client is None:
        logger.error(f"Could not access sync redis client from broker for {hostname}")
        return

    try:
        redis_conn = redis_broker.client
        key = service_health_timestamps_key()
        current_timestamp = int(time.time())
        redis_conn.hset(key, hostname, current_timestamp)
        logger.debug(f'Health ping processed for hostname: {hostname} at {current_timestamp}')
    except Exception as e:
        logger.error(f"Error in health_ping update for hostname {hostname}: {e}")


def create_health_ping_actor(broker: RedisBroker, queue_name: str, actor_name: str, logger: logger):
    """Factory function to create a worker-specific health ping Dramatiq actor.
    
    Args:
        broker: The Dramatiq RedisBroker instance.
        queue_name: The specific health queue name for this worker.
        actor_name: The unique Dramatiq actor name for the health ping.
        logger: The bound logger instance for the specific worker.
    """

    @dramatiq.actor(broker=broker, queue_name=queue_name, actor_name=actor_name)
    # Actor only accepts arguments passed via .send()
    def generated_health_ping_actor(hostname: str):
        """Dramatiq actor created by the factory."""
        try:
            # Use broker and logger from the enclosing factory scope (closure)
            update_health_timestamp_with_client(hostname, broker, logger)
        except Exception as e:
            # Use the logger from closure here too
            logger.error(f"Failed to execute health ping update logic in {actor_name}: {e}")

    return generated_health_ping_actor


async def run_periodic_broker_health_check(
    logger, # Bound logger instance
    # Remove broker parameter, it's not needed here
    # broker: RedisBroker, 
    redis_conn: aioredis.Redis,
    hostname: str,
    health_report_interval: int,
    health_actor_send, # The specific .send method (e.g., health_ping_snapshot.send)
    worker_type: str, # e.g., "SnapshotWorker", "Aggregator", "Distributor"
    health_queue_name: str
):
    """Generic coroutine for periodically triggering and checking Dramatiq worker liveness."""
    # (Ensure body is correctly indented)
    logger.info(
        f'Starting periodic {worker_type} health check task for {hostname} (Interval: {health_report_interval}s)',
    )
    # Threshold for considering worker unresponsive
    dramatiq_liveness_threshold = health_report_interval + 30
    health_key = service_health_timestamps_key()

    # Allow a grace period on startup before reporting critical errors
    startup_grace_period_end = time.time() + dramatiq_liveness_threshold + 10
    
    while True:
        try:
            
            health_actor_send(hostname)
            logger.debug(f"Sent {worker_type} health ping for {hostname} to {health_queue_name}")

            last_ping_time_bytes = await redis_conn.hget(health_key, hostname)
            current_time = int(time.time())

            if last_ping_time_bytes:
                last_ping_time = int(last_ping_time_bytes.decode())
                time_since_last_ping = current_time - last_ping_time
                if time_since_last_ping > dramatiq_liveness_threshold:
                    # Only log critical after grace period
                    if current_time > startup_grace_period_end:
                        logger.critical(
                            f"{worker_type} Dramatiq workers seem unresponsive for {hostname}. Last health ping acknowledged {time_since_last_ping}s ago"
                            f" (Threshold: {dramatiq_liveness_threshold}s). Key: {health_key}, Field: {hostname}"
                        )
                    else:
                         logger.debug(
                            f"{worker_type} Dramatiq workers potentially unresponsive for {hostname} (in startup grace period). Last health ping acknowledged {time_since_last_ping}s ago."
                         )
                else:
                    logger.info(
                        f"{worker_type} Dramatiq workers appear responsive for {hostname}. Last health ping acknowledged {time_since_last_ping}s ago."
                    )
            else:
                # If the key/field doesn't exist yet, maybe the first ping hasn't been processed.
                 if current_time > startup_grace_period_end:
                    logger.debug(
                        f"{worker_type} Dramatiq worker health timestamp for {hostname} not found. Workers might be starting up or unresponsive. Key: {health_key}"
                    )
                 else:
                    logger.debug(
                         f"{worker_type} Dramatiq worker health timestamp for {hostname} not yet found (in startup grace period). Key: {health_key}"
                     )

            await asyncio.sleep(health_report_interval)
        except asyncio.CancelledError:
            logger.info(f'Periodic {worker_type} health reporter task for {hostname} cancelled.')
            break
        except Exception as e:
            logger.error(f'Error in periodic {worker_type} health reporter loop: {e}')
            # Avoid tight loop on error
            await asyncio.sleep(health_report_interval)


async def run_periodic_task_health_check(
    logger, # Bound logger instance
    redis_conn: aioredis.Redis, 
    hostname: str, 
    health_report_interval: int,
    main_task: asyncio.Task # The main task to monitor (e.g., _unpin_snapshots_task)
):
    """Generic coroutine for periodically reporting health status for non-Dramatiq task-based workers.
    
    Checks if the provided main_task is running and updates the service health timestamp.
    """
    logger.info(
        f'Starting periodic health reporter task for {hostname} (Interval: {health_report_interval}s)',
    )
    while True:
        should_report = True
        task_exception = None
        
        if not main_task or main_task.done():
            should_report = False
            if main_task:
                # Task is done, check for exception
                try:
                    task_exception = main_task.exception()
                except asyncio.CancelledError:
                     logger.warning(f'Main task for {hostname} was cancelled. Halting health reports.')
                     break # Halt reporter if main task is cancelled
                except Exception as e:
                    # Should ideally be caught by main_task.exception() but as fallback
                    logger.error(f'Error retrieving exception from main task for {hostname}: {e}. Halting health reports.')
                    break # Halt reporter if exception retrieval fails

                if task_exception:
                    logger.error(
                        f'Main task for {hostname} failed with exception: {task_exception}. Halting health reports.'
                    )
                else:
                    logger.warning(
                        f'Main task for {hostname} finished unexpectedly. Halting health reports.'
                    )
                # Halt the health reporter if the main task is done (failed or finished)
                break

        try:
            if should_report:
                current_timestamp = int(time.time())
                await redis_conn.hset(
                    service_health_timestamps_key(),
                    hostname,
                    current_timestamp,
                )
                logger.debug(f'Reported health for {hostname} at {current_timestamp}')

            await asyncio.sleep(health_report_interval)
        except asyncio.CancelledError:
            logger.info(f'Periodic health reporter task for {hostname} cancelled.')
            break
        except Exception as e:
            logger.error(f'Error in periodic health reporter loop for {hostname}: {e}')
            # Avoid tight loop on error
            await asyncio.sleep(health_report_interval)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        hostname_arg = socket.gethostname()
        if not hostname_arg:
            print('Usage: python health_ping.py <hostname_to_check>', file=sys.stderr)
            print('Error: Hostname argument is required for Docker healthcheck.', file=sys.stderr)
            sys.exit(2)  # Usage error
    else:
        hostname_arg = sys.argv[1]

    # Set up the event loop (uvloop is often used in your project)
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    # Run the health check
    is_healthy = loop.run_until_complete(check_health(hostname_arg))

    # Exit with 0 for healthy, 1 for unhealthy
    sys.exit(0 if is_healthy else 1)
