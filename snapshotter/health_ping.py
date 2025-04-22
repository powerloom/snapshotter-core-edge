import json
import socket
import sys
import time
from asyncio import set_event_loop

import uvloop
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
            service_health_timestamps_key,
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
    set_event_loop(loop)

    # Run the health check
    is_healthy = loop.run_until_complete(check_health(hostname_arg))

    # Exit with 0 for healthy, 1 for unhealthy
    sys.exit(0 if is_healthy else 1)
