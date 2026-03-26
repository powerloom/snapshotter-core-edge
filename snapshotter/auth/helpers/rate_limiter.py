"""
Async Redis-backed rate limiting used by ``helpers.py`` (``generic_rate_limiter``).

Not imported by ``server_entry.py`` or the Core API until you wire
``Depends(...)`` from ``helpers.py`` onto routes. See module docstring on
``helpers.py`` for wiring status.
"""

import time
from typing import List

import redis.exceptions
from async_limits import RateLimitItem
from async_limits.storage import AsyncRedisStorage
from async_limits.strategies import AsyncFixedWindowRateLimiter
from redis import asyncio as aioredis

# Initialize rate limits when program starts
LUA_SCRIPT_SHAS = None

# # # RATE LIMITER LUA SCRIPTS

# Script to clear all keys matching a pattern
SCRIPT_CLEAR_KEYS = """
        local keys = redis.call('keys', KEYS[1])
        local res = 0
        for i=1,#keys,5000 do
            res = res + redis.call(
                'del', unpack(keys, i, math.min(i+4999, #keys))
            )
        end
        return res
        """

# Script to increment a key and set its expiry
SCRIPT_INCR_EXPIRE = """
        local current
        current = redis.call("incrby",KEYS[1],ARGV[2])
        if tonumber(current) == tonumber(ARGV[2]) then
            redis.call("expire",KEYS[1],ARGV[1])
        end
        return current
    """

# Script to set a key's value and expiry
# args = [value, expiry]
SCRIPT_SET_EXPIRE = """
    local keyttl = redis.call('TTL', KEYS[1])
    local current
    current = redis.call('SET', KEYS[1], ARGV[1])
    if keyttl == -2 then
        redis.call('EXPIRE', KEYS[1], ARGV[2])
    elseif keyttl ~= -1 then
        redis.call('EXPIRE', KEYS[1], keyttl)
    end
    return current
"""

# # # END RATE LIMITER LUA SCRIPTS


async def load_rate_limiter_scripts(redis_conn: aioredis.Redis):
    """
    Load rate limiter scripts into Redis and return their SHA hashes.

    This function should be run only once at the start of the application.

    Args:
        redis_conn (aioredis.Redis): Redis connection object.

    Returns:
        dict: A dictionary containing the SHA hashes of the loaded scripts.
    """
    script_clear_keys_sha = await redis_conn.script_load(SCRIPT_CLEAR_KEYS)
    script_incr_expire = await redis_conn.script_load(SCRIPT_INCR_EXPIRE)
    return {
        'script_incr_expire': script_incr_expire,
        'script_clear_keys': script_clear_keys_sha,
    }


async def generic_rate_limiter(
    parsed_limits: List[RateLimitItem],
    key_bits: list,
    redis_conn: aioredis.Redis,
    rate_limit_lua_script_shas=None,
    limit_incr_by=1,
):
    """
    A generic rate limiter that uses Redis as a storage backend.

    Args:
        parsed_limits (List[RateLimitItem]): A list of RateLimitItem objects that define the rate limits.
        key_bits (list): A list of key bits to be used as part of the Redis key.
        redis_conn (aioredis.Redis): An instance of aioredis.Redis that is used to connect to Redis.
        rate_limit_lua_script_shas (dict, optional): A dictionary containing the SHA hashes of the Lua scripts used by the rate limiter.
        limit_incr_by (int, optional): The amount by which to increment the rate limit counter. Defaults to 1.

    Returns:
        tuple: A tuple containing:
            - bool: Indicating whether the rate limit check passed
            - int: The retry-after time in seconds
            - str: A string representation of the rate limit that was checked

    Raises:
        Exception: If there's an error with Redis operations
    """
    if not rate_limit_lua_script_shas:
        rate_limit_lua_script_shas = await load_rate_limiter_scripts(redis_conn)
    redis_storage = AsyncRedisStorage(rate_limit_lua_script_shas, redis_conn)
    custom_limiter = AsyncFixedWindowRateLimiter(redis_storage)
    for each_lim in parsed_limits:
        try:
            if await custom_limiter.hit(each_lim, limit_incr_by, *[key_bits]) is False:
                window_stats = await custom_limiter.get_window_stats(
                    each_lim,
                    key_bits,
                )
                reset_in = 1 + window_stats[0]
                retry_after = reset_in - int(time.time())
                return False, retry_after, str(each_lim)
        except (
            redis.exceptions.ConnectionError,
            redis.exceptions.TimeoutError,
            redis.exceptions.ResponseError,
        ) as exc:
            raise Exception from exc
    return True, 0, ''
