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
        logger.error(f"Received None redis_broker in health update for {logger.module}")
        return
    if not hasattr(redis_broker, 'client') or redis_broker.client is None:
        logger.error(f"Could not access sync redis client from broker for {logger.module}")
        return

    try:
        redis_conn = redis_broker.client
        key = service_health_timestamps_key()
        current_timestamp = int(time.time())
        redis_conn.hset(key, hostname, current_timestamp)
        logger.debug(f'{logger.module} health ping processed for hostname: {hostname} at {current_timestamp}')
    except Exception as e:
        logger.error(f"Error in {logger.module} health_ping update for hostname {hostname}: {e}")


def create_health_ping_actor(broker: RedisBroker, queue_name: str, actor_name: str):
    """Factory function to create a worker-specific health ping Dramatiq actor."""

    @dramatiq.actor(broker=broker, queue_name=queue_name, actor_name=actor_name)
    def generated_health_ping_actor(hostname: str, broker: RedisBroker, logger: logger):
        """Dramatiq actor created by the factory."""
        try:
            update_health_timestamp_with_client(hostname, logger, broker)
        except Exception as e:
            logger.error(f"Failed to execute health ping update logic in {logger.module}: {e}")

    return generated_health_ping_actor


async def run_periodic_health_check(
    logger, # Bound logger instance
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
            # Send a ping message with our hostname to the dedicated health queue
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
                    logger.debug(
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
